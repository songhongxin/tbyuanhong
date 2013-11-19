package com.etao.lz.star.output.broadcast;

import com.etao.lz.star.Tools;
import com.etao.lz.star.monitor.WangWangAlert;
import com.google.protobuf.GeneratedMessage;

public class IgnoreState implements BroadcastState {
	private long ignoreCount;
	private WangWangAlert alert;
	private long lastNotify;
	private String id;

	public IgnoreState(String id, String wwNotify) {
		this.id = id;
		if (wwNotify != null) {
			alert = new WangWangAlert(wwNotify);
		}
		lastNotify = 0;
	}

	@Override
	public void addData(byte[] data, GeneratedMessage log) {
		++ignoreCount;
		long time = System.currentTimeMillis();
		if (time - lastNotify < 1000 * 60 * 10)
			return;
		if (alert != null) {
			alert.alert(id + " Broadcast Alert", String.format(
					"%s starts to abandon sending logs to subscriber %s",
					Tools.getIdAndHost(), id));
			lastNotify = time;
		}
	}

	public long getIgnoreCount() {
		return ignoreCount;
	}
	
}
