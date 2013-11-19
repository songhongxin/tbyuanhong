package com.etao.lz.star.bolt;

import java.util.Map;

import com.etao.lz.star.StarConfig;
import com.etao.lz.star.StarLogProtos;
import com.google.protobuf.GeneratedMessage.Builder;

@SuppressWarnings("rawtypes") 
public class AtPanelFillProcessor implements LogProcessor {

	private static final long serialVersionUID = 166953321682088499L;
	@Override
	public boolean accept(Builder builder) {
		StarLogProtos.FlowStarLog.Builder log = (StarLogProtos.FlowStarLog.Builder) builder;
		return log.getDestType().equals("browse");
	}

	@Override
	public void config(StarConfig config) {

	}

	@Override
	public void open(int taskId) {

	}

	@Override
	public void process(Builder builder) {
		StarLogProtos.FlowStarLog.Builder log = (StarLogProtos.FlowStarLog.Builder) builder;
		String sid = log.getSid();
		int index = sid.indexOf('_');
		if(index>0)
		{
			sid = sid.substring(0,index);
		}
		log.setSid(sid);
		
		String uid_mid = null;
		String puid = null;
		String uid = log.getUid();
		if(uid.length() > 0 && !uid.equals("-") && !uid.trim().equals("0"))
		{
			uid_mid = log.getUid();
			puid = log.getUid() + ":";
		}
		else
		{
			uid_mid = log.getMid();		
			puid = ":" + sid;
		}
		log.setUidMid(uid_mid);
		log.setPuid(puid);

	}

	@Override
	public void getStat(Map<String, String> map) {
	}

}
