package com.deloitte.beam.schema;

import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;

@DefaultSchema(JavaFieldSchema.class)
public class Address {

	public final String city;
	public final String street;
	public final int pincode;
	@SchemaCreate
	public Address(String city, String street, int pincode) {
		super();
		this.city = city;
		this.street = street;
		this.pincode = pincode;
	}
	
}
