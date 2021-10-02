package com.gallop.connect.logminer.source.model;

import com.gallop.connect.logminer.source.LogMinerSourceConnectorConstants;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import java.util.Collections;
import java.util.Map;

public class LogMinerEvent {
	private final Schema schema;
	private final Struct struct;

	public LogMinerEvent(Schema schema, Struct struct) {
		this.schema = schema;
		this.struct = struct;
	}

	public Schema getSchema() {
		return this.schema;
	}

	public Struct getStruct() {
		return this.struct;
	}

	public Map<String, Object> getOffset() {
		//Timestamp.toLogical(struct.schema().field(LogMinerSourceConnectorConstants.FIELD_TIMESTAMP).schema(),
		//						((java.sql.Timestamp) struct.get(LogMinerSourceConnectorConstants.FIELD_TIMESTAMP)).getTime())
		return new Offset(struct.getInt64(LogMinerSourceConnectorConstants.FIELD_SCN),
				struct.getInt64(LogMinerSourceConnectorConstants.FIELD_COMMIT_SCN),
				struct.getString(LogMinerSourceConnectorConstants.FIELD_ROW_ID)
				).toMap();
	}

	public Offset getOffsetObject(){
		return new Offset(struct.getInt64(LogMinerSourceConnectorConstants.FIELD_SCN),
				struct.getInt64(LogMinerSourceConnectorConstants.FIELD_COMMIT_SCN),
				struct.getString(LogMinerSourceConnectorConstants.FIELD_ROW_ID)
		);
	}

	public Map<String, String> getPartition() {
		return Collections.singletonMap(LogMinerSourceConnectorConstants.TABLE_NAME_KEY, schema.name());
	}

	@Override
	public String toString() {
		return "LogMinerEvent [schema=" + schema + ", struct=" + struct + "]";
	}
}
