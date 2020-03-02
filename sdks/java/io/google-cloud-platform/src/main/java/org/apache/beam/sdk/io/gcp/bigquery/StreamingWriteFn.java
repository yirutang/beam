/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.gcp.bigquery;

import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.cloud.bigquery.storage.v1alpha2.ProtoBufProto;
import com.google.protobuf.*;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.SinkMetrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.SystemDoFnInternal;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.ShardedKey;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.joda.time.Instant;

import com.google.cloud.bigquery.storage.v1alpha2.BigQueryWriteClient;
import com.google.api.gax.rpc.BidiStream;
import com.google.cloud.bigquery.storage.v1alpha2.Storage.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1alpha2.Storage.AppendRowsRequest;
import com.google.cloud.bigquery.storage.v1alpha2.Storage.CreateWriteStreamRequest;
import com.google.cloud.bigquery.storage.v1alpha2.Stream.WriteStream;
import com.google.cloud.bigquery.storage.v1alpha2.ProtoBufProto.ProtoSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.DescriptorProtos;

import java.util.HashMap;

/**
 * ProtobufEnvelope - allows creating a protobuf message without the .proto file dynamically.
 *
 * @author Florian Leibert
 */
class ProtobufEnvelope {
  private HashMap<String, Object> values = new HashMap();
  private DescriptorProtos.DescriptorProto.Builder desBuilder;
  private int i = 1;

  public ProtobufEnvelope() {
    desBuilder = DescriptorProtos.DescriptorProto.newBuilder();
    i = 1;
  }

  public <T> void addField(String fieldName, T fieldValue, DescriptorProtos.FieldDescriptorProto.Type type) {
    DescriptorProtos.FieldDescriptorProto.Builder fd1Builder = DescriptorProtos.FieldDescriptorProto.newBuilder()
                                                                       .setName(fieldName).setNumber(i++).setType(type);
    desBuilder.addField(fd1Builder.build());
    values.put(fieldName, fieldValue);
  }

  public Message constructMessage(String messageName)
          throws Descriptors.DescriptorValidationException {
    desBuilder.setName(messageName);
    DescriptorProtos.DescriptorProto dsc = desBuilder.build();

    DescriptorProtos.FileDescriptorProto fileDescP = DescriptorProtos.FileDescriptorProto.newBuilder()
                                                             .addMessageType(dsc).build();

    Descriptors.FileDescriptor[] fileDescs = new Descriptors.FileDescriptor[0];
    Descriptors.FileDescriptor dynamicDescriptor = Descriptors.FileDescriptor.buildFrom(fileDescP, fileDescs);
    Descriptors.Descriptor msgDescriptor = dynamicDescriptor.findMessageTypeByName(messageName);
    DynamicMessage.Builder dmBuilder =
            DynamicMessage.newBuilder(msgDescriptor);
    for (String name : values.keySet()) {
      dmBuilder.setField(msgDescriptor.findFieldByName(name), values.get(name));
    }
    return dmBuilder.build();
  }

  public void clear() {
    desBuilder = DescriptorProtos.DescriptorProto.newBuilder();
    i = 1;
    values.clear();
  }
}
/** Implementation of DoFn to perform streaming BigQuery write. */
@SystemDoFnInternal
@VisibleForTesting
class StreamingWriteFn<ErrorT, ElementT>
    extends DoFn<KV<ShardedKey<String>, TableRowInfo<ElementT>>, Void> {
  private static final Logger LOG = LoggerFactory.getLogger(StreamingWriteFn.class);
  private final BigQueryServices bqServices;
  private final InsertRetryPolicy retryPolicy;
  private final TupleTag<ErrorT> failedOutputTag;
  private final ErrorContainer<ErrorT> errorContainer;
  private final boolean skipInvalidRows;
  private final boolean ignoreUnknownValues;
  private final boolean ignoreInsertIds;
  private final SerializableFunction<ElementT, TableRow> toTableRow;
  private final boolean useNewApi;
  private String streamName;

  /** JsonTableRows to accumulate BigQuery rows in order to batch writes. */
  private transient Map<String, List<ValueInSingleWindow<TableRow>>> tableRows;

  /** The list of unique ids for each BigQuery table row. */
  private transient Map<String, List<String>> uniqueIdsForTableRows;

  /** Tracks bytes written, exposed as "ByteCount" Counter. */
  private Counter byteCounter = SinkMetrics.bytesWritten();

  StreamingWriteFn(
      BigQueryServices bqServices,
      InsertRetryPolicy retryPolicy,
      TupleTag<ErrorT> failedOutputTag,
      ErrorContainer<ErrorT> errorContainer,
      boolean skipInvalidRows,
      boolean ignoreUnknownValues,
      boolean ignoreInsertIds,
      boolean useNewApi,
      SerializableFunction<ElementT, TableRow> toTableRow) {
    this.bqServices = bqServices;
    this.retryPolicy = retryPolicy;
    this.failedOutputTag = failedOutputTag;
    this.errorContainer = errorContainer;
    this.skipInvalidRows = skipInvalidRows;
    this.ignoreUnknownValues = ignoreUnknownValues;
    this.ignoreInsertIds = ignoreInsertIds;
    this.useNewApi = useNewApi;
    this.toTableRow = toTableRow;
  }

  /** Prepares a target BigQuery table. */
  @StartBundle
  public void startBundle() {
    tableRows = new HashMap<>();
    uniqueIdsForTableRows = new HashMap<>();
    // Create a stream if using new API
    if (this.useNewApi) {
      BigQueryWriteClient writeapi = null;
      try {
        writeapi = BigQueryWriteClient.create();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      CreateWriteStreamRequest request =
              CreateWriteStreamRequest.newBuilder().setParent(
                      "projects/bigquerytestdefault/datasets/samples/tables/yiru_new_writeapi")
                      .setWriteStream(WriteStream.newBuilder().setType(WriteStream.Type.COMMITTED).build()).build();
      this.streamName = writeapi.createWriteStream(request).getName();
      LOG.info("Created WriteStream: " + this.streamName);
    }
  }

  /** Accumulates the input into JsonTableRows and uniqueIdsForTableRows. */
  @ProcessElement
  public void processElement(
      @Element KV<ShardedKey<String>, TableRowInfo<ElementT>> element,
      @Timestamp Instant timestamp,
      BoundedWindow window,
      PaneInfo pane) {

    String tableSpec = element.getKey().getKey();
    List<ValueInSingleWindow<TableRow>> rows =
        BigQueryHelpers.getOrCreateMapListValue(tableRows, tableSpec);
    List<String> uniqueIds =
        BigQueryHelpers.getOrCreateMapListValue(uniqueIdsForTableRows, tableSpec);

    TableRow tableRow = toTableRow.apply(element.getValue().tableRow);
    rows.add(ValueInSingleWindow.of(tableRow, timestamp, window, pane));
    uniqueIds.add(element.getValue().uniqueId);
  }

  /** Writes the accumulated rows into BigQuery with streaming API. */
  @FinishBundle
  public void finishBundle(FinishBundleContext context) throws Exception {
    BigQueryWriteClient writeapi = null;
    try {
       writeapi = BigQueryWriteClient.create();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    BidiStream<AppendRowsRequest, AppendRowsResponse> bidiStream = writeapi.appendRowsCallable().call();

    List<ValueInSingleWindow<ErrorT>> failedInserts = Lists.newArrayList();
    BigQueryOptions options = context.getPipelineOptions().as(BigQueryOptions.class);
    for (Map.Entry<String, List<ValueInSingleWindow<TableRow>>> entry : tableRows.entrySet()) {
      TableReference tableReference = BigQueryHelpers.parseTableSpec(entry.getKey());
      if (useNewApi) {
        flushRowsWriteApi(
                bidiStream,
                tableReference,
                entry.getValue(),
                uniqueIdsForTableRows.get(entry.getKey()),
                options,
                failedInserts);
      } else {
        flushRows(
                tableReference,
                entry.getValue(),
                uniqueIdsForTableRows.get(entry.getKey()),
                options,
                failedInserts);
      }
    }
    tableRows.clear();
    uniqueIdsForTableRows.clear();

    for (ValueInSingleWindow<ErrorT> row : failedInserts) {
      context.output(failedOutputTag, row.getValue(), row.getTimestamp(), row.getWindow());
    }
    if (useNewApi) {
      bidiStream.closeSend();
    }
  }

  /** Writes the accumulated rows into BigQuery with streaming API. */
  private void flushRows(
      TableReference tableReference,
      List<ValueInSingleWindow<TableRow>> tableRows,
      List<String> uniqueIds,
      BigQueryOptions options,
      List<ValueInSingleWindow<ErrorT>> failedInserts)
      throws InterruptedException {
    if (!tableRows.isEmpty()) {
      LOG.info("Insert Rows " + tableRows.size());
      try {
        long totalBytes =
            bqServices
                .getDatasetService(options)
                .insertAll(
                    tableReference,
                    tableRows,
                    uniqueIds,
                    retryPolicy,
                    failedInserts,
                    errorContainer,
                    skipInvalidRows,
                    ignoreUnknownValues,
                    ignoreInsertIds);
        byteCounter.inc(totalBytes);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private void flushRowsWriteApi(
          BidiStream<AppendRowsRequest, AppendRowsResponse> bidiStream,
          TableReference tableReference,
          List<ValueInSingleWindow<TableRow>> tableRows,
          List<String> uniqueIds,
          BigQueryOptions options,
          List<ValueInSingleWindow<ErrorT>> failedInserts)
          throws InterruptedException {
    if (!tableRows.isEmpty()) {
      ProtobufEnvelope pe = new ProtobufEnvelope();

      AppendRowsRequest.Builder requestBuilder = AppendRowsRequest.newBuilder();

      AppendRowsRequest.ProtoData.Builder dataBuilder = AppendRowsRequest.ProtoData.newBuilder();
      dataBuilder.setWriterSchema(ProtoSchema.newBuilder().setProtoDescriptor(
              DescriptorProtos.DescriptorProto.newBuilder().setName("Message").addField(
                      DescriptorProtos.FieldDescriptorProto.newBuilder().setName("foo").setType(
                              DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING).setNumber(1).build()).build()));

      ProtoBufProto.ProtoRows.Builder rows = ProtoBufProto.ProtoRows.newBuilder();
      for (ValueInSingleWindow<TableRow> row : tableRows) {
        pe.addField("foo", row.getValue().get("foo").toString(),
                DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING);
        try {
        rows.addSerializedRows(pe.constructMessage("t").toByteString());
        } catch (Descriptors.DescriptorValidationException e) {
          throw new RuntimeException(e);
        }
        pe.clear();
      }
      dataBuilder.setRows(rows);
      while (!bidiStream.isSendReady()) { Thread.sleep(1000); }
      LOG.info("Sending rows " + tableRows.size());
      bidiStream.send(requestBuilder.setOffset(
              Int64Value.of(-1)).setWriteStream(streamName).setProtoRows(dataBuilder.build()).build());
      LOG.info("Checking response " + tableRows.size());
      AppendRowsResponse response = bidiStream.iterator().next();
      LOG.info("Get response:" + response.hasError() + " " + response.getOffset());
    }
  }
}
