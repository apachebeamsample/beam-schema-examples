package com.deloitte.beam.schema;

import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;

@DefaultSchema(JavaFieldSchema.class)
public class Student {

	
	public final String firstName;
	
	public final String lastName;
	
	public final int totalMarks;
	
	public final Address address;
	@SchemaCreate
	public Student(String firstName, String lastName, int totalMarks, Address address) {
		super();
		this.firstName = firstName;
		this.lastName = lastName;
		this.totalMarks = totalMarks;
		this.address = address;
	}
}
