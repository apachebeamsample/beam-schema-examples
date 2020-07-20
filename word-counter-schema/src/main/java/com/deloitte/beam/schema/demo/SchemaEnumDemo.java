package com.deloitte.beam.schema.demo;

import java.util.Arrays;

import org.apache.beam.sdk.schemas.logicaltypes.EnumerationType;

public class SchemaEnumDemo {

	public static void main(String[] args) {
		
		EnumerationType enumType  = EnumerationType
		.create(Arrays.asList("Apple","Banana","Mango"));
		
		//enumType.getValues().forEach(v->System.out.println(v));
		enumType.getValuesMap().forEach((k,v)->{System.out.println(v+":"+k);});;
	}

}
