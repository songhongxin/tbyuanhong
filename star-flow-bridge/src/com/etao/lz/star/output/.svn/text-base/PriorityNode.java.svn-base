package com.etao.lz.star.output;

import com.etao.lz.star.StarConfig;
import com.etao.lz.star.StarLogProtos.BusinessStarLog;
import com.etao.lz.star.StarLogProtos.FlowStarLog;
import com.google.protobuf.GeneratedMessage;

public class PriorityNode implements Comparable<PriorityNode>{
	private GeneratedMessage  log;
    private byte[] bytes;
    private StarConfig config;
    
    private static StarConfig FlowConfig = new StarConfig("flow-const");
    private static StarConfig BusinessConfig = new StarConfig("business-const");
    static{
    	FlowConfig.addAttribute("LogType", "flow");
    	BusinessConfig.addAttribute("LogType", "business");
    }
	public PriorityNode(GeneratedMessage log,byte[] bytes, StarConfig config) {
		this.log = log;
		this.bytes = bytes;
		this.config = config;
	}
	
	public PriorityNode(FlowStarLog log) {
		this.log = log;
		this.bytes = null;
		this.config = FlowConfig;
	}
	
	public PriorityNode(BusinessStarLog log) {
		this.log = log;
		this.bytes = null;
		this.config = BusinessConfig;
	}
	
	public GeneratedMessage getLog() {
		return log;
	}
	
	public byte[] getBytes() {
		return bytes;
	}
	
	@Override
	public int compareTo(PriorityNode o) {
		long diff =  config.getLogTime(log) - config.getLogTime(o.getLog());
		if(diff >0)
		{
			return 1;
		}
		else if (diff < 0)
		{
			return -1;
		}
		else
		{
			return 0;
		}
	}

	public long getTime() {
		return config.getLogTime(log);
	}

}
