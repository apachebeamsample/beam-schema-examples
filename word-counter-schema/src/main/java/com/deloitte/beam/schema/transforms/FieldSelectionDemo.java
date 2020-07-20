package com.deloitte.beam.schema.transforms;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import com.deloitte.beam.schema.Employee;

public class FieldSelectionDemo {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		PipelineOptions options = PipelineOptionsFactory.create();
		Pipeline pipeline = Pipeline.create(options);
		PCollection<Employee> employeePColl =
				readEmployees(pipeline);
		
		employeePColl.apply("fieldSelection", ParDo.of(new DoFn<Employee,Employee>() {
			
			@ProcessElement
			public void process(@FieldAccess("firstName") String firstName,
					@FieldAccess("lastName") String lastName) {
				
				System.out.println(lastName+","+firstName);
			}
		}));
		
		pipeline.run().waitUntilFinish();
	}

	private static PCollection<Employee> readEmployees(Pipeline pipeline) {
		Employee employee = new Employee("Pradeep","Ravindran",20000);
		Employee employee2 = new Employee("Ramesh","Krishna",30000);
		return pipeline.apply(Create.of(employee,employee2));
	}

}
