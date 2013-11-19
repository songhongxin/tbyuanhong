package com.etao.lz.star.monitor;

public class StatInfo {
	private long lastReceivedLogTime;

	private long count;

	public StatInfo()
	{
		count = 1;
		lastReceivedLogTime = 0;
	}
	
	public void update()
	{
		++count;
	}
	
	public void setLastReceivedLogTime(long lastReceivedLogTime) {
		this.lastReceivedLogTime = lastReceivedLogTime;
	}
	
	public long getLastReceivedLogTime() {
		return lastReceivedLogTime;
	}
	
	public long getCount() {
		return count;
	}

	public void reset() {
		count = 0;
	}
}
