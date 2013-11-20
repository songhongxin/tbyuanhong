package com.etao.lz.storm;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Byte数组包装类，支持Byte数组按内容比较，按内容计算哈希值。
 * 
 * @author jiuling.ypf
 * 
 */
public class ByteArrayWrapper implements Comparable<ByteArrayWrapper> {
	protected byte[] data;

	public ByteArrayWrapper() {
	}

	public ByteArrayWrapper(byte[] data) {
		if (data == null) {
			throw new NullPointerException();
		}
		this.data = data;
	}

	@Override
	public boolean equals(Object other) {
		if (!(other instanceof ByteArrayWrapper)) {
			return false;
		}
		return Bytes.equals(data, ((ByteArrayWrapper) other).data);
	}

	@Override
	public int hashCode() {
		return Bytes.hashCode(data);
	}

	@Override
	public int compareTo(ByteArrayWrapper other) {
		return Bytes.compareTo(data, other.data);
	}

	public byte[] getData() {
		return data;
	}

}
