package com.deloitte.beam.schema;

import java.time.Instant;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.LogicalType;
import org.apache.beam.sdk.values.Row;

public class TimestampNanos implements LogicalType<Instant,Row> {

	private final Schema schema = Schema.builder()
			.addInt64Field("seconds").addInt32Field("nanos")
			.build();
	@Override
	public String getIdentifier() {
		return this.getClass().getSimpleName().toLowerCase();
	}

	@Override
	public FieldType getArgumentType() {
		return FieldType.DATETIME;
	}

	@Override
	public FieldType getBaseType() {
			return FieldType.DATETIME;
	}

	@Override
	public Row toBaseType(Instant input) {
		Instant instant = Instant.now();
		return Row.withSchema(schema).addValues(instant.getEpochSecond(), instant.getNano()).build();
	}

	@Override
	public Instant toInputType(Row base) {
		return Instant.ofEpochSecond(base.getInt64("seconds").longValue(), base.getInt32("nanos").longValue());
	}

}
