package com.deloitte.beam.word.counter;

import java.util.Arrays;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

/**
 * Hello world!
 *
 */
public class WordCounter 
{
    public static void main( String[] args )
    {
        PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
        Pipeline p = Pipeline.create(pipelineOptions);
        PCollection<KV<String,Long>> wordCount = p
        		.apply("(1) Read all lines", TextIO.read().from("src/main/resources/input.txt"))
        		.apply("(2) FlatMap to a list of words",
        				FlatMapElements.into(TypeDescriptors.strings())
        				.via(line -> Arrays.asList(line.split("\\s"))))
        		.apply("(3) Lowercase all",
        				MapElements.into(TypeDescriptors.strings())
        				.via(word -> word.toLowerCase()))
        		.apply("(4)", Count.perElement());
        
        // First, we convert our PCollection to String. 
        // Then, we use TextIO to write the output:
        
        wordCount.apply(MapElements.into(TypeDescriptors.strings())
        		.via(count -> count.getKey() + " --> " + count.getValue()))
        .apply(TextIO.write().to("src/main/resources/output"));
        
        p.run().waitUntilFinish();
    }
}
