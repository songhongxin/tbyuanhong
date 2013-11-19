package com.etao.lz.star.output.hbase;

import com.google.protobuf.GeneratedMessage;

public interface RowKeyBuilder {
	public byte[] build(GeneratedMessage log, short taskId);

	public byte[] buildIndex(GeneratedMessage minLog, short taskId);
}
