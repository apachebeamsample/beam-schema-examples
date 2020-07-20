package com.deloitte.beam.schema.transforms;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import com.deloitte.beam.schema.Address;
import com.deloitte.beam.schema.Student;

public class NestedFieldSelectionDemo {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		PipelineOptions options = PipelineOptionsFactory
				.create();
		Pipeline pipeline = Pipeline.create(options);
		
		PCollection<Student> studentsPColl = readStudents(pipeline);
		
		studentsPColl.apply("nestedSelection", ParDo.of(new DoFn<Student,Student>(){
			
			@ProcessElement
			public void process(@FieldAccess("firstName") String firstName,
					@FieldAccess("address.city")String city,
					@FieldAccess("address.pincode")int pincode) {
				System.out.println(firstName+":"+city+":"+pincode);
			}
		}));
		
		pipeline.run().waitUntilFinish();
	}

	private static PCollection<Student> readStudents(Pipeline pipeline) {
		Address a1 = new Address("Bangalore","AECS",560037);
		Address a2 = new Address("Mysore","KD Road",570002);
		Student s1 = new Student("Pradeep","Ravindran",500,a1);
		Student s2 = new Student("Karthik","Narayan",600,a2);
		return pipeline.apply(Create.of(s1, s2));
	}

}
