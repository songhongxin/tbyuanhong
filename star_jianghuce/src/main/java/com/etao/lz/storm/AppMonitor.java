package com.etao.lz.storm;

import java.util.Map;

import backtype.storm.task.TopologyContext;

import com.etao.lz.star.ZKTools;
import com.etao.lz.star.monitor.TTMonitor;


/*
 * 
 * 习性大法监控的
 * 
 * */
public class AppMonitor extends TTMonitor {

	/**
	 * 序列化id
	 */
	private static final long serialVersionUID = 379630524261628616L;
	private long _count;
	private long _ts;
	private String _zkServers;
	private String _topologyName;
	private String _name;
	private int _interval;
	private int _taskId;
	
	
	@SuppressWarnings("rawtypes")
	public AppMonitor(String name, Map conf, TopologyContext context) {
		_taskId = context.getThisTaskId();
		_zkServers = conf.get("zkServers").toString();
		_topologyName = conf.get("topologyName").toString();
		_name = name;
		_interval = Integer.parseInt(conf.get("monitorInterval").toString());
	}
	
	public void open() {
		this.open(_interval, _taskId, _topologyName, _name, ZKTools.startCuratorClient(_zkServers));
	}
	
	@Override
	protected long getEmitCount() {
		return _count;
	}

	@Override
	protected long getLastTupleTime() {
		return _ts;
	}

	@Override
	protected int getQueueSize() {
		
	//	return _queue.size();
		return -1;
	}

	@Override
	protected void reset() {
		_count = 0;
	}
	
	public void increase() {
		_count++;
	}
	
	public void sign(long ts) {
		_ts = ts;
		_count++;
	}
	
	public void setTs(long ts) {
		_ts = ts;
	}
	
	public long getTs() {
		return _ts;
	}

	@Override
	protected void updateStat() {}
}
