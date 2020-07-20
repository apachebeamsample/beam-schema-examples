package com.deloitte.beam.schema.demo;

import java.util.Arrays;

import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.logicaltypes.OneOfType;

public class SchemaOneOfTypeDemo {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		OneOfType oneOfType = OneOfType.create(
				Field.of("age",FieldType.INT32)
				,Field.of("name", FieldType.STRING));
		
		OneOfType.Value oneOfValue = oneOfType.createValue("name", "Pradeep");
		System.out.println(oneOfValue.getValue());
	}

}
