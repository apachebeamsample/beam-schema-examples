package com.deloitte.beam.schema.demo;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import com.deloitte.beam.schema.Transaction;
import com.deloitte.beam.schema.TransactionPojo;

public class FirstSchemaDemo {

	public static void main(String[] args) {
		
		PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
		Pipeline pipeline = Pipeline.create(pipelineOptions);
	
		PCollection<Transaction> transactionBean = readTransactionAsJavaBean(pipeline);
		PCollection<TransactionPojo> transactionPojo = readTransactionAsPojo(pipeline);
		
		PCollection<Transaction> transactionBean2 =transactionBean.apply(ParDo.of(new DoFn<Transaction,Transaction>(){
			
				
			@ProcessElement	public void process(@Element TransactionPojo transactionPojo) {
					
				transactionPojo.setBank("HSBC");
				System.out.println(transactionPojo);
				
			}
		}));
		
		pipeline.run().waitUntilFinish();
	}

	private static PCollection<TransactionPojo> readTransactionAsPojo(Pipeline pipeline) {
		TransactionPojo pojo = new TransactionPojo("Hdfc",1000d);
		return pipeline.apply(Create.of(pojo));
	}

	private static PCollection<Transaction> readTransactionAsJavaBean(Pipeline pipeline) {
			Transaction transactionBean = new Transaction("citi",2500.50d);
			return pipeline.apply(Create.of(transactionBean));
		
	}

}
