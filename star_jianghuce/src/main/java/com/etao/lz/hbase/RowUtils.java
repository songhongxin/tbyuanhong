package com.etao.lz.hbase;

import java.util.zip.CRC32;

import org.apache.hadoop.hbase.util.Bytes;

import com.etao.lz.storm.tools.Constants;

public class RowUtils {

	public static class RptDpt {

		// rowkey-hour :=
		// <prefix/1><sellerid/8><date/4><timeinterval/1><auctionid/8>
		public static byte[] rowKeyHour(int date, byte timeinterval,
				long seller_id, long auction_id) {
			byte[] row = new byte[1 + 8 + 4 + 1 + 8];
			byte shardingkey = rowKeyPrefix(seller_id);
			Bytes.putByte(row, 0, shardingkey);
			Bytes.putLong(row, 1, seller_id);
			Bytes.putInt(row, 8 + 1, date);
			Bytes.putByte(row, 4 + 8 + 1, timeinterval);
			Bytes.putLong(row, 4 + 8 + 1 + 1, auction_id);
			return row;
		}

		// rowkey-day:= <prefix/1><sellerid/8><date/4><auctionid/8>
		public static byte[] rowKeyDay(int date, long seller_id, long auction_id) {
			byte[] row = new byte[1 + 8 + 4 + 8];
			byte shardingkey = rowKeyPrefix(seller_id);
			Bytes.putByte(row, 0, shardingkey);
			Bytes.putLong(row, 1, seller_id);
			Bytes.putInt(row, 8 + 1, date);
			Bytes.putLong(row, 4 + 8 + 1, auction_id);
			return row;
		}

		// 获取分区前缀
		public static byte rowKeyPrefix(long sellerid) {
			CRC32 crc32 = new CRC32();
			byte[] b_sellerid = Bytes.toBytes(sellerid);
			crc32.update(b_sellerid, 0, 8);
			byte prefix = (byte) (crc32.getValue() % Constants.BIGB_HBASE_SHARDING_NUM);

			return prefix;
		}

	}
	/*
	 * public static void main(String[] args) {
	 * 
	 * byte p = RptDpt.rowKeyPrefix(123L); int x = p; System.out.println(p);
	 * System.out.println(x);
	 * 
	 * }
	 */
}
