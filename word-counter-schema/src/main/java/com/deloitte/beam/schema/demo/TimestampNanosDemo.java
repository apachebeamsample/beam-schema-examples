package com.deloitte.beam.schema.demo;

import java.time.Instant;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

import com.deloitte.beam.schema.TimestampNanos;

public class TimestampNanosDemo {

	public static void main(String[] args) {
		
		PipelineOptions options = PipelineOptionsFactory.create();
		Pipeline pipeline = Pipeline.create(options);
		PCollection<TimestampNanos> pc = pipeline.apply(Create.of(new TimestampNanos()));
		pc.apply(ParDo.of(new DoFn<TimestampNanos,TimestampNanos>() {
			
			@ProcessElement	
			public void process(@Element TimestampNanos timestamp) {
				Row row = timestamp.toBaseType(Instant.now());
				System.out.println(row);
				Instant instant = timestamp.toInputType(row);
				System.out.println(instant);
			}
		}));
		
		pipeline.run().waitUntilFinish();
	}

}
