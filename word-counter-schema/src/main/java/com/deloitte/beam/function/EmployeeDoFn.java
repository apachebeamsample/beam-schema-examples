package com.deloitte.beam.function;

import org.apache.beam.sdk.transforms.DoFn;

import com.deloitte.beam.schema.Employee;

public class EmployeeDoFn extends DoFn<Employee, Employee> {

	@ProcessElement	public void process(@Element Employee employee) {
		System.out.println(employee);
	}
}
