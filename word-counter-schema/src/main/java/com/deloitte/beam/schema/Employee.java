package com.deloitte.beam.schema;

import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;

@DefaultSchema(JavaFieldSchema.class)
public class Employee {
	
	public final String firstName;
	public final String lastName;
	public final int salary;
		
	
	@SchemaCreate
	public Employee(String firstName,String lastName,int salary) {
		this.firstName = firstName;
		this.lastName = lastName;
		this.salary = salary;
	}

	// Getter setters omitted because of Schema Create
	// Beam will understand that it has to use this constructor
	// for creating the object
	
	@Override
	public String toString() {
		return this.firstName
				+":" + this.lastName
				+":" + this.salary;
	}
}
