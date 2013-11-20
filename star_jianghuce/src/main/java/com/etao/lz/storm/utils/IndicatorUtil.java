package com.etao.lz.storm.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.hadoop.hbase.util.Bytes;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.etao.lz.star.StarLogProtos.BusinessStarLog;

public class IndicatorUtil {
	private static final Logger log = LoggerFactory
			.getLogger(IndicatorUtil.class);

	public static long toPosIndKey(int bcd, int spmCrc32) {
		byte[] b1 = Bytes.toBytes(bcd);
		byte[] b2 = Bytes.toBytes(spmCrc32);
		byte[] rb = Bytes.add(b1, b2);
		return Bytes.toLong(rb);
	}

	/**
	 * 解析 PosIndKey 为 bcd 和 spmCrc32
	 * 
	 * @param pk
	 * @return [0] 为 bcd，[1] 为 spmCrc32
	 */
	public static int[] parsePosIndKey(long pk) {
		byte[] b = Bytes.toBytes(pk);
		int bcd = Bytes.toInt(b, 0, 4);
		int spmCrc32 = Bytes.toInt(b, 4, 4);
		int[] r = { bcd, spmCrc32 };
		return r;
	}

	public static long auctionIdToAsid(long auctionId) {
		return auctionId * 10 + 1;
	}

	public static long sellerIdToAsid(long sellerId) {
		return sellerId * 10 + 2;
	}

	public static class Asid {
		public long id;
		public boolean isSellerId;
	}

	public static Asid decodeAsid(long asid) {
		Asid res = new Asid();
		if (asid % 10 == 2) {
			res.isSellerId = true;
		} else {
			res.isSellerId = false;
		}
		res.id = asid / 10;
		return res;
	}

	/**
	 * 将日期时间转换为内部小时级数据时段标识
	 * 
	 * 例如 <code>2012-10-23 03:45:03</code> 会在忽略分秒部分后被转换为 <code>1350932400</code>
	 * 
	 * @param dt
	 * @return
	 */
	public static int dateTimeToBcd(DateTime dt) {
		return (int) (dt.withMinuteOfHour(0).withSecondOfMinute(0)
				.withMillisOfSecond(0).getMillis() / 1000);
	}

	/**
	 * 将ms级epoch时戳转换为内部小时级数据时段标识
	 * 
	 * 例如 <code>1350935103000</code> 会转换为 <code>1350932400</code>
	 * 
	 * @param epoch
	 * @return
	 */
	public static int epochMsToBcd(long epoch) {
		return (int) (new DateTime(epoch).withMinuteOfHour(0)
				.withSecondOfMinute(0).withMillisOfSecond(0).getMillis() / 1000);
	}

	/**
	 * 从内部小时级数据时段标识中提取当天日期 00:00:00 对应的 epoch 时戳
	 * 
	 * @param bcd
	 * @return
	 */
	public static int bcdToDateEpoch(int bcd) {
		DateTime dt = new DateTime(bcd * 1000L);
		return (int) (dt.withHourOfDay(0).withMinuteOfHour(0)
				.withSecondOfMinute(0).withMillisOfSecond(0).getMillis() / 1000);
	}

	/**
	 * 从 epoch 时戳提取 YYYYmmdd 整数
	 * 
	 * @param epoch
	 * @return
	 */
	public static int epochToYmd(int epoch) {
		DateTime dt = new DateTime(epoch * 1000L);
		return dt.getYear() * 10000 + dt.getMonthOfYear() * 100
				+ dt.getDayOfMonth();
	}

	/**
	 * 从内部小时级数据时段标识中提取当天内时段(0~23)
	 * 
	 * @param bcd
	 * @return
	 */
	public static byte bcdToTid(int bcd) {
		DateTime dt = new DateTime(bcd * 1000L);
		return (byte) dt.getHourOfDay();
	}

	public static Object unserializeObject(byte[] data) {
		if (data == null) {
			return null;
		}
		Object o = null;
		try {
			ByteArrayInputStream bais = new ByteArrayInputStream(data);
			ObjectInputStream ois = new ObjectInputStream(bais);
			o = ois.readObject();
			ois.close();
		} catch (Exception e) {
			log.error("Failed to unserialize bytes to object", e);
		}
		return o;
	}

	public static byte[] serializeObject(Object o) {
		byte[] r = null;
		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(baos);
			oos.writeObject(o);
			oos.close();
			r = baos.toByteArray();
		} catch (Exception e) {
			log.error("Failed to serialize object to bytes", e);
		}
		return r;
	}

	public static void logBizSpec(Logger logger, String mark,
			BusinessStarLog biz) {
		if ("228784630".equals(biz.getSellerId())) {
			log.info(String.format("%s : %s", mark, biz.toString()));
		}
	}

	public static void main(String[] args) throws Exception {
		System.out.println(epochToYmd(1384142400));
	}
}
