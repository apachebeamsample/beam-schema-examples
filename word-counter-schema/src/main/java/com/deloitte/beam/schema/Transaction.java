package com.deloitte.beam.schema;

import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

@DefaultSchema(JavaBeanSchema.class)
public class Transaction {

	private String bank;
	private double purchaseAmount;
	
	public Transaction() {
		this.bank = "dummy";
		this.purchaseAmount=0d;
	}
	
	public Transaction(String bank, double purchaseAmount) {
		super();
		this.bank = bank;
		this.purchaseAmount = purchaseAmount;
	}

	public String getBank() {
		return bank;
	}

	public void setBank(String bank) {
		this.bank = bank;
	}

	public double getPurchaseAmount() {
		return purchaseAmount;
	}

	public void setPurchaseAmount(double purchaseAmount) {
		this.purchaseAmount = purchaseAmount;
	}
	
	@Override
	public String toString() {
		return "Bank:"+getBank() +" purchaseAmount:"+getPurchaseAmount();
	}
}
