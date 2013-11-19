package com.etao.lz.star.output.hbase;

import java.util.zip.CRC32;

import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.GeneratedMessage;

public abstract class AbstractRowkeyBuilder implements RowKeyBuilder {


	private static final long serialVersionUID = -4853176003304980713L;
	private short sequenceNo = 0;
	@Override
	public byte[] build(GeneratedMessage glog, short taskId) {
		String puid = getPuid(glog);
		if (puid == null) {
			return null;
		}
		int ts = getTime(glog);
		sequenceNo = (short) (sequenceNo + 1);
		CRC32 crc32 = new CRC32();
		crc32.update(Bytes.toBytes(puid));
		byte shardingKey = (byte) (crc32.getValue() % HBaseTableHelper.SHARDING_KEY_NUM);

		byte[] bShardingKey = { shardingKey };
		byte[] bTs = Bytes.toBytes(ts);
		byte[] bPuid = Bytes.toBytes(puid);
		byte[] bTaskId = Bytes.toBytes(taskId);
		byte[] bSeq = Bytes.toBytes(sequenceNo);

		byte[] rowKey1 = Bytes.add(bShardingKey, bTs, bPuid);
		byte[] rowKey = Bytes.add(rowKey1, bTaskId, bSeq);

		return rowKey;
	}

	protected abstract String getPuid(GeneratedMessage glog);
	protected abstract int getTime(GeneratedMessage glog);
}
