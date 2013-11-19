package com.etao.lz.star.bolt;

import java.util.Map;

import com.etao.lz.star.StarConfig;
import com.google.protobuf.GeneratedMessage;

@SuppressWarnings("rawtypes")
public interface LogProcessor extends java.io.Serializable{
	
	public void open(int taskId);

	public boolean accept(GeneratedMessage.Builder log);

	public void config(StarConfig config);
    

	public void process( GeneratedMessage.Builder builder);

	public void getStat(Map<String, String> map);
}
