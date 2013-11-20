package com.etao.lz.storm.detailcalc;

public class LogItem {
	
	
	
	public long getTs() {
		return ts;
	}

	public void setTs(long ts) {
		this.ts = ts;
	}

	public String getSellerid() {
		return sellerid;
	}

	public void setSellerid(String sellerid) {
		this.sellerid = sellerid;
	}

	public byte[] getLogs() {
		return logs;
	}

	public void setLogs(byte[] logs) {
		this.logs = logs;
	}

	private  long  ts;
	
	private  String  sellerid;
	
	private  byte[]  logs;
	
	
	

}
