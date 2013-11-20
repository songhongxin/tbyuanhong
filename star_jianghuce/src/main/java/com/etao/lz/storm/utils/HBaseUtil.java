package com.etao.lz.storm.utils;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;

public class HBaseUtil {

	/**
	 * 对给定的 HBase 位图字段进行去重合并后转换为 AdaptiveCounting 对象
	 * 
	 * @param cur
	 * @param ac
	 * @param family
	 * @param qualifier
	 * @return
	 */
	public static AdaptiveCounting uniqAddHBaseCol(Logger log, Result cur,
			AdaptiveCounting ac, byte[] family, byte[] qualifier) {
		byte[] fval = null;
		if (cur != null) {
			fval = cur.getValue(family, qualifier);
		}

		AdaptiveCounting res = ac;
		if (fval != null) {
			try {
				res = (AdaptiveCounting) res.merge(new AdaptiveCounting(fval));
			} catch (Exception e) {
				if (log != null)
					log.error("Failed to unique merge HBase column", e);
			}
		}
		return res;
	}

	/**
	 * 对给定 HBase 字段增加给定值后转换为 byte[]
	 * 
	 * @param cur
	 * @param up
	 * @param family
	 * @param qualifier
	 * @return
	 */
	public static byte[] addHBaseCol(Logger log, Result cur, Object up,
			byte[] family, byte[] qualifier) {
		byte[] fval = null;
		if (cur != null) {
			fval = cur.getValue(family, qualifier);
		}

		if (up instanceof Integer) {
			int u = (Integer) up;
			if (fval != null) {
				u += Bytes.toInt(fval);
			}
			fval = Bytes.toBytes(u);
		} else if (up instanceof Long) {
			long u = (Long) up;
			if (fval != null) {
				u += Bytes.toLong(fval);
			}
			fval = Bytes.toBytes(u);
		} else if (up instanceof Float) {
			float u = (Float) up;
			if (fval != null) {
				u += Bytes.toFloat(fval);
			}
			fval = Bytes.toBytes(u);
		} else if (up instanceof String) {
			String u = (String) up;
			if (fval != null) {
				u = Bytes.toString(fval);
			}
			fval = Bytes.toBytes(u);
		} else {
			if (log != null)
				log.error(String.format("Given data type is not addable: %s",
						up.getClass().toString()));
			throw new RuntimeException("invalid data type");
		}

		return fval;
	}

}
