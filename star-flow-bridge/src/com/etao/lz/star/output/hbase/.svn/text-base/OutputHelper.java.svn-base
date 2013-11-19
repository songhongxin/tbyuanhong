package com.etao.lz.star.output.hbase;

import java.io.IOException;

import com.google.protobuf.GeneratedMessage;

public interface OutputHelper {

	public abstract void flush() throws IOException;

	public abstract void write(byte[] rowKey, byte[] data, GeneratedMessage log)
			throws IOException;

}