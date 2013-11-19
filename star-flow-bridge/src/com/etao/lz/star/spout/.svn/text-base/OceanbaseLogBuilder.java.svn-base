package com.etao.lz.star.spout;

import com.etao.lz.star.StarLogProtos.BusinessStarLog;
import com.google.protobuf.GeneratedMessage;

public class OceanbaseLogBuilder extends FieldsLogBuilder {
	@SuppressWarnings("rawtypes")
	protected boolean buildLog(String tag, String[] log_fields,
			GeneratedMessage.Builder gbuilder) throws LogFormatException {
		BusinessStarLog.Builder builder = (BusinessStarLog.Builder) gbuilder;
		String dbAction = null;
		int tsIndex = log_fields[0].indexOf(':');
		long ts = 0;
		if (tsIndex > 0) {
			String time = log_fields[0].substring(0, tsIndex);
			ts = Long.parseLong(time);
			ts = ts / 1000;
		}

		dbAction = log_fields[2].toLowerCase();
		if (dbAction.startsWith("delete"))
			return false;
		builder.setOrderModifiedT(ts);
		builder.setDbAction(dbAction);
		return super.buildLog(tag, log_fields, builder);
	}
}
