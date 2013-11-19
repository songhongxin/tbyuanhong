package com.etao.lz.star.output.hbase;

import com.etao.lz.star.StarLogProtos.FlowStarLog;
import com.google.protobuf.GeneratedMessage;

public class FlowRowKeyBuilder extends AbstractRowkeyBuilder {

	private static final long serialVersionUID = 9078372264626967891L;

	@Override
	public byte[] buildIndex(GeneratedMessage minLog, short taskId) {
		return null;
	}

	@Override
	protected String getPuid(GeneratedMessage glog) {
		FlowStarLog log = (FlowStarLog) glog;
		String psuid = log.getPuid();
		if (psuid.equals(":-") || psuid.equals(":")) {
			return null;
		}
		return psuid;
	}

	@Override
	protected final int getTime(GeneratedMessage glog) {
		FlowStarLog log = (FlowStarLog) glog;
		int ts =  (int) (log.getTs() / 1000);
		ts = ts % (HBaseAdminHelper.WEEK_DAYS * HBaseAdminHelper.DAY_HOURS * HBaseAdminHelper.HOUR_SECONDS);
		return ts;
	}
}
