package com.etao.lz.star.spout;

import java.util.HashMap;
import java.util.concurrent.BlockingQueue;

import com.etao.lz.star.StarConfig;

public interface LogGenerator extends java.io.Serializable{

	void config(StarConfig config);

	void open(BlockingQueue<byte[]> queue, String name, int thisTaskId);

	void getStat(HashMap<String, String> map);

	long getLastTupleTime();
}
