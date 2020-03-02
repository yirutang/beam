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
package org.apache.beam.examples;

import org.apache.beam.examples.common.ExampleUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;

public class NewWriter {
	public interface WriterOptions extends PipelineOptions {

		/**
		 * By default, this example reads from a public dataset containing the text of King Lear. Set
		 * this option to choose a different input file or glob.
		 */
		@Description("Path of the file to read from")
		@Default.String("gs://yiru-dataflow/test/test_100m")
		String getInputFile();

		void setInputFile(String value);

		/** Set this required option to specify where to write the output. */
		@Description("Path of the file to write to")
		@Required
		String getOutput();

		void setOutput(String value);
	}

	static void runWrite(WriterOptions options) {
		Pipeline p = Pipeline.create(options);

		TableSchema schema =
				new TableSchema()
						.setFields(
								ImmutableList.of(new TableFieldSchema().setName("foo").setType("STRING")));

		BigQueryIO.Write write =
				BigQueryIO.write()
						.to("bigquerytestdefault:samples.yiru_new_write")
						.withFormatFunction(
								str -> {
									return new TableRow().set("foo", str);
								})
						.withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
						.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
						.withSchema(schema)
						.useNewApi()
						.withoutValidation();
		p.apply("ReadLines", TextIO.read().from(options.getInputFile())).apply("Streaming", write);
		p.run().waitUntilFinish();
	}

	public static void main(String[] args) {
		WriterOptions options =
				PipelineOptionsFactory.fromArgs(args).withValidation().as(WriterOptions.class);

		runWrite(options);
	}
}
