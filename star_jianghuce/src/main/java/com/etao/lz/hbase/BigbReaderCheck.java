package com.etao.lz.hbase;

import java.io.IOException;
import java.util.NavigableMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.etao.lz.storm.tools.Constants;
import com.etao.lz.storm.utils.TimeUtils;

/*
 * 暂时不用了
 * */

public class BigbReaderCheck {

	// 水绕田园HBase表列簇名
	private static final byte[] columnFamily = Bytes
			.toBytes(Constants.BIGB_HBASE_FAMILY);

	private static final byte[] alipayamt = Bytes.toBytes("alipayamt");
	// 水绕田园HBase表列名--支付宝成交笔数
	private static final byte[] alipaynum = Bytes.toBytes("alipaynum");

	public static final String tablenameD = Constants.DP_D_TABLE;
	public static final String tablenameH = Constants.DP_H_TABLE;

	public static final Log log = LogFactory.getLog(BigbReaderCheck.class);

	// HBase客户端配置对象
	private static final Configuration conf = HBaseConfiguration.create();

	// 从hbase中读取店铺的数据状态
	public static void readUnit(long sellerid, long auctionid, long ts)
			throws IOException {

		int date = Integer.parseInt(TimeUtils.getTimeDate(ts));
		System.out.println("time of hbase : " + date);
		// byte timeinterval = Byte.parseByte(TimeUtils.getTimeHour(ts));

		byte[] key = RowUtils.RptDpt.rowKeyDay(date, sellerid, auctionid);

		HTable hTable = new HTable(conf, tablenameD);

		Get get = new Get(key);

		get.addFamily(columnFamily);

		try {
			Result re = hTable.get(get);

			if (re == null) {
				log.error("[getUnitValues] hbase.result is null");
				return;
			}

			if (re.isEmpty()) {
				log.error("[getUnitValues] hbase.result is empty");
				return;
			}

			// 从family层级 开始寻找colum , value

			NavigableMap<byte[], byte[]> map = re.getFamilyMap(columnFamily);

			if (map == null || map.size() < 1) {
				log.error("[getUnitValues] map.result is empty");
				return;
			}

			// 读取状态

			System.out.println("get sellerid  : " + String.valueOf(sellerid));
			System.out.println("get alipayamt: "
					+ String.valueOf(Bytes.toLong(map.get(alipayamt))));
			System.out.println("get alipaynum: "
					+ String.valueOf(Bytes.toLong(map.get(alipaynum))));

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void main(String[] args) throws IOException {

		if (args.length < 3) {
			System.out.println("please input sellerid and auctionid: ");
			System.exit(0);
		}

		long sellerid = Long.parseLong(args[0]);
		long auctionid = Long.parseLong(args[1]);
		long ts = Long.parseLong(args[1]);
		BigbReaderCheck.readUnit(sellerid, auctionid, ts);
	}

}
