package com.etao.lz.star.output.hbase;

import java.util.zip.CRC32;

import org.apache.hadoop.hbase.util.Bytes;

import com.etao.lz.star.StarLogProtos.BusinessStarLog;
import com.google.protobuf.GeneratedMessage;

public class PayOrderRowKeyBuilder extends BusinessRowkeyBuilder {

	private static final long serialVersionUID = 9078372264626967891L;

	@Override
	public byte[] buildIndex(GeneratedMessage glog, short taskId) {
		BusinessStarLog log = (BusinessStarLog) glog;
		String bizOrderId = log.getOrderId();

		byte[] bBizOrderId = Bytes.toBytes(bizOrderId);
		
		CRC32 crc32			= new CRC32();
		crc32.update(Bytes.toBytes(bizOrderId));
		byte shardingKey	= (byte)(crc32.getValue() % 32);
		
		byte[] bShardingKey	= {shardingKey};
		byte[] rowKey = Bytes.add(bShardingKey, bBizOrderId);

		return rowKey;
	}
}
