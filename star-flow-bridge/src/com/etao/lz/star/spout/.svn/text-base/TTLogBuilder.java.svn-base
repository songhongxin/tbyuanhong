package com.etao.lz.star.spout;

import java.util.List;

import com.etao.lz.star.StarConfig;
import com.google.protobuf.GeneratedMessage;

public interface TTLogBuilder {
	public List<GeneratedMessage> build(String tag, byte[] data);

	public void config(StarConfig starConfig);
	
	public int getErrorCount();

	public String getLastErrorMsg();
}
