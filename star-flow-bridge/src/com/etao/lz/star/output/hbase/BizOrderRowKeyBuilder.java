package com.etao.lz.star.output.hbase;

import java.util.zip.CRC32;

import org.apache.hadoop.hbase.util.Bytes;

import com.etao.lz.star.StarLogProtos.BusinessStarLog;
import com.google.protobuf.GeneratedMessage;

public class BizOrderRowKeyBuilder extends BusinessRowkeyBuilder {

	private static final long serialVersionUID = 9078372264626967891L;

	@Override
	public byte[] buildIndex(GeneratedMessage glog, short taskId) {
		BusinessStarLog log = (BusinessStarLog) glog;
		String psuid		= getPuid(glog);
		if (psuid == null) return null;
		
		CRC32 crc32			= new CRC32();
		crc32.update(Bytes.toBytes(psuid));
		byte shardingKey	= (byte)(crc32.getValue() % 32);
		
		String parentId		= log.getParentId();
		if (parentId.equals("0")) {
			return null;
		}
		String bizOrderId	= log.getOrderId();
		
		byte[] bShardingKey	= {shardingKey};
		byte[] bParentId	= Bytes.toBytes(parentId);
		byte[] bBizOrderId	= Bytes.toBytes(bizOrderId);
		
		byte[] rowKey		= Bytes.add(bShardingKey, bParentId, bBizOrderId);
		
		return rowKey;
	}
	

}
