package com.etao.lz.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.NavigableMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.etao.lz.storm.detailcalc.UnitItem;
import com.etao.lz.storm.detailcalc.UserAccessEntry;
import com.etao.lz.storm.tools.Constants;
import com.etao.lz.storm.utils.AdaptiveCounting;
import com.etao.lz.storm.utils.TimeUtils;

/*
 * 水绕田园  ：  hbase客户端实例生成和对应接口，用于detail_bolt指标计算。
 * 
 * */

public class HbaseManager implements java.io.Serializable {

	/**
	 * 序列号
	 */
	private static final long serialVersionUID = -3329946899504217374L;

	// 日志操作记录对象
	private static final Log LOG = LogFactory.getLog(HbaseManager.class);

	// 水绕田园HBase表名
	private final String tableName;

	// 水绕田园HBase表列簇名
	private static final byte[] columnFamily = Bytes
			.toBytes(Constants.BIGB_HBASE_FAMILY);

	private static final byte[] pv = Bytes.toBytes("pv"); // 浏览量
	private static final byte[] alipayAmt = Bytes.toBytes("amt"); // 支付宝成交金额
	private static final byte[] alipayNum = Bytes.toBytes("an"); // 支付宝成交件数
	private static final byte[] alipayTradeNum = Bytes.toBytes("atn"); // 成交笔数
	private static final byte[] tradeNum = Bytes.toBytes("tn"); // 拍下笔数
	private static final byte[] auv = Bytes.toBytes("auv"); // 成交uv
	private static final byte[] iuv = Bytes.toBytes("iuv"); // 流量uv
	private static final byte[] ab = Bytes.toBytes("ab"); // 成交uv位图信息 byte[] <
															// 8192
	private static final byte[] ib = Bytes.toBytes("ib"); // 流量uv位图信息 byte[] <
															// 8192

	private static final byte[] az = Bytes.toBytes("az"); // 当日支付率（atn/tn）
	private static final byte[] ar = Bytes.toBytes("ar"); // 成交转化率（auv/iuv）
	private static final byte[] au = Bytes.toBytes("au"); // 访客价值（amt/iuv）

	// HBase客户端配置对象
	private transient Configuration conf;

	// HTable客户端对象
	private transient HTable hTable;

	// 客户端缓存大小
	private final int rptGrdWBufSize = 67108864;

	/**
	 * 构造函数
	 * 
	 * @param tableName
	 * 
	 */
	public HbaseManager(String tableName) {
		this.tableName = tableName;

		Configuration cconf = new Configuration();
		cconf.addResource("hbase-site-new.xml");
		// cconf.addResource("hbase-site.xml");

		conf = HBaseConfiguration.create(cconf);

		initHTable();
	}

	/**
	 * 初始化客户端HTable对象，用户从HBase读取数据
	 * 
	 * @return
	 */
	private boolean initHTable() {
		try {
			hTable = new HTable(conf, tableName);

			hTable.setWriteBufferSize(rptGrdWBufSize);
			hTable.setAutoFlush(false);
		} catch (IOException e) {
			LOG.error("Initialize htable error, table name: " + tableName, e);
			return false;
		}
		return true;
	}

	// 从hbase中读取宝贝全天的数据状态
	public void readItemDay(UserAccessEntry item, long sellerId,
			long auctionId, long ts) {

		int date = Integer.parseInt(TimeUtils.getTimeDate(ts));
		byte[] key = RowUtils.RptDpt.rowKeyDay(date, sellerId, auctionId);

		Get get = new Get(key);
		get.addFamily(columnFamily);

		try {

			Result re = hTable.get(get);

			if (re == null) {
				LOG.error("[getDUnitValues] hbase.result is null");
				return;
			}
			if (re.isEmpty()) {
				LOG.error("[getDUnitValues] hbase.result is empty");
				return;
			}

			// 从family层级 开始寻找colum , value
			NavigableMap<byte[], byte[]> map = re.getFamilyMap(columnFamily);

			if (map == null || map.size() < 1) {
				LOG.error("[getDUnitValues] map.result is empty");
				return;
			}

			// 读取状态
			item.sellerId = sellerId;
			item.alipayNum = Bytes.toLong(map.get(alipayNum));
			item.alipayAmt = Bytes.toLong(map.get(alipayAmt));
			item.pv = Bytes.toLong(map.get(pv));
			item.alipayTradeNum = Bytes.toLong(map.get(alipayTradeNum));
			item.tradeNum = Bytes.toLong(map.get(tradeNum));

			if (map.get(ib) != null) {
				item.ib = new AdaptiveCounting(map.get(ib)); // 会检测是否采用稀疏位图的模式
			}

			if (map.get(ab) != null) {
				item.ab = new AdaptiveCounting(map.get(ab));
			}

		} catch (Exception e) {
			e.printStackTrace();
			LOG.error("[getDUnitValues] map.result exception");
		}

	}

	// 从hbase中读取宝贝时段的数据状态
	public void readItemHour(UserAccessEntry item, long sellerId,
			long auctionId, long ts) {

		int date = Integer.parseInt(TimeUtils.getTimeDate(ts));
		byte timeinterval = Byte.parseByte(TimeUtils.getTimeHour(ts));

		byte[] key = RowUtils.RptDpt.rowKeyHour(date, timeinterval, sellerId,
				auctionId);

		Get get = new Get(key);

		get.addFamily(columnFamily);

		try {
			Result re = hTable.get(get);

			if (re == null) {
				LOG.error("[getHUnitValues] hbase.result is null");
				return;
			}
			if (re.isEmpty()) {
				LOG.error("[getHUnitValues] hbase.result is empty");
				return;
			}

			// 从family层级 开始寻找colum , value
			NavigableMap<byte[], byte[]> map = re.getFamilyMap(columnFamily);

			if (map == null || map.size() < 1) {
				LOG.error("[getHUnitValues] map.result is empty");
				return;
			}

			// 读取状态
			item.alipayNum = Bytes.toLong(map.get(alipayNum));
			item.alipayAmt = Bytes.toLong(map.get(alipayAmt));
			item.pv = Bytes.toLong(map.get(pv));
			item.alipayTradeNum = Bytes.toLong(map.get(alipayTradeNum));
			item.tradeNum = Bytes.toLong(map.get(tradeNum));

			if (map.get(ib) != null) {
				item.ib = new AdaptiveCounting(map.get(ib)); // 会检测是否采用稀疏位图的模式
			}

			if (map.get(ab) != null) {
				item.ab = new AdaptiveCounting(map.get(ab));
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	// 存储一批店铺的状态到hbase中
	public boolean putUnit(ArrayList<UnitItem> list, long ts, String mark)
			throws IOException {

		ArrayList<Put> putlist = getPutList(list, ts, mark);

		if (putlist.size() > 0) {

			try {

				long bts = System.currentTimeMillis();
				long size = putlist.size();
				hTable.put(putlist);
				hTable.flushCommits();
				long ats = System.currentTimeMillis();
				LOG.info("Put list to hbase success, the size is " + size
						+ ", use " + (ats - bts) + " millisseconds.");

			} catch (Exception e) {

				LOG.error("Failed to put list to hbase.");
				return false;

			}
		}

		return true;

	}

	// 获取需要更新到hbase中的批量宝贝数据
	private ArrayList<Put> getPutList(ArrayList<UnitItem> list, long ts,
			String mark) {

		ArrayList<Put> putlist = new ArrayList<Put>();

		int day = Integer.parseInt(TimeUtils.getTimeDate(ts));
		byte timeinterval = Byte.parseByte(TimeUtils.getTimeHour(ts));

		for (int i = 0; i < list.size(); i++) {

			UnitItem item = list.get(i);
			byte[] key;
			if (mark.equals("h")) {
				key = RowUtils.RptDpt.rowKeyHour(day, timeinterval,
						item.sellerId, item.auctionId);
			} else {
				key = RowUtils.RptDpt.rowKeyDay(day, item.sellerId,
						item.auctionId);
			}

			byte[] Tpv = Bytes.toBytes(item.pv);
			byte[] TalipayAmt = Bytes.toBytes(item.alipayAmt);
			byte[] TalipayNum = Bytes.toBytes(item.alipayNum);
			byte[] TalipayTradeNum = Bytes.toBytes(item.alipayTradeNum);
			byte[] TtradeNum = Bytes.toBytes(item.tradeNum);

			byte[] har = Bytes.toBytes(item.dealRate);
			byte[] hau = Bytes.toBytes(item.uvValue);
			byte[] haz = Bytes.toBytes(item.alipayRate);

			byte[] hiuv = Bytes.toBytes(item.ib.cardinality());
			byte[] hauv = Bytes.toBytes(item.ab.cardinality());
			byte[] hab = item.ab.getBytes();
			byte[] hib = item.ib.getBytes();

			Put put = new Put(key);

			put.add(columnFamily, pv, Tpv);
			put.add(columnFamily, alipayAmt, TalipayAmt);
			put.add(columnFamily, alipayNum, TalipayNum);
			put.add(columnFamily, alipayTradeNum, TalipayTradeNum);
			put.add(columnFamily, tradeNum, TtradeNum);
			put.add(columnFamily, ar, har);
			put.add(columnFamily, au, hau);
			put.add(columnFamily, az, haz);
			put.add(columnFamily, auv, hauv);
			put.add(columnFamily, iuv, hiuv);

			if (hab != null) {
				put.add(columnFamily, ab, hab);
			} else {
				LOG.error("hab is null !!");
			}
			if (hib != null) {
				put.add(columnFamily, ib, hib);
			} else {
				LOG.error("hib is null !!");
			}

			putlist.add(put);

		}
		LOG.info("Mark " + mark + " put list to hbase sucess!!");
		return putlist;
	}

	/*
	 * public static void main(String [] args){
	 * 
	 * byte [] b = new byte[8]; long a = Bytes.toLong(b); System.out.println(a);
	 * 
	 * }
	 */

}