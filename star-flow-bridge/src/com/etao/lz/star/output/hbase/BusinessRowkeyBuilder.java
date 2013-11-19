package com.etao.lz.star.output.hbase;

import com.etao.lz.star.StarLogProtos.BusinessStarLog;
import com.google.protobuf.GeneratedMessage;

public class BusinessRowkeyBuilder extends AbstractRowkeyBuilder {

	private static final long serialVersionUID = 3599645036178226290L;

	@Override
	protected String getPuid(GeneratedMessage glog) {
		BusinessStarLog log = (BusinessStarLog) glog;
		String psuid = log.getBuyerId();
		if (psuid.equals("0") || psuid.equals("-")) {
			return null;
		}
		return psuid + ":";
	}

	@Override
	protected final int getTime(GeneratedMessage glog) {
		BusinessStarLog log = (BusinessStarLog) glog;
		int ts = (int) (log.getOrderModifiedT() / 1000);
		ts = ts % (HBaseAdminHelper.WEEK_DAYS * HBaseAdminHelper.DAY_HOURS * HBaseAdminHelper.HOUR_SECONDS);
		return ts;
	}

	@Override
	public byte[] buildIndex(GeneratedMessage minLog, short taskId) {
		// TODO Auto-generated method stub
		return null;
	}

}
