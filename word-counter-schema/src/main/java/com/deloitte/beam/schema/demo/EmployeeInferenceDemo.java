package com.deloitte.beam.schema.demo;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import com.deloitte.beam.function.EmployeeDoFn;
import com.deloitte.beam.schema.Employee;

public class EmployeeInferenceDemo {

	public static void main(String[] args) {
		
		PipelineOptions options = PipelineOptionsFactory.create();
		Pipeline pipeline = Pipeline.create(options);
		
		PCollection<Employee> employeePColl = readEmployees(pipeline);
		employeePColl.apply(ParDo.of(new EmployeeDoFn()));
		
		pipeline.run().waitUntilFinish();
	}

	private static PCollection<Employee> readEmployees(Pipeline pipeline) {
		Employee employee = new Employee("Pradeep","Ravindran",20000);
		Employee employee2 = new Employee("Ramesh","Krishna",30000);
		return pipeline.apply(Create.of(employee,employee2));
	}

}
