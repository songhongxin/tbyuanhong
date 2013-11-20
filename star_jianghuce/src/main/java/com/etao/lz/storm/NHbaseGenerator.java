package com.etao.lz.storm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.joda.time.DateTime;

/**
 * HBase数据流生成类，支持吸星大法中按秒级时间戳的流量日志读取！！
 * scanMainTableTs  方法是新增
 * 
 * @author yuanhong.shx
 * 
 */

public class NHbaseGenerator implements java.io.Serializable{

	// 序列化ID号
	private static final long serialVersionUID = -8470991851015791082L;

	// 日志操作记录对象
	private static final Log LOG = LogFactory.getLog(NHbaseGenerator.class);

	// 吸星大法HBase表名
	private String tableName;

	// 吸星大法HBase表列簇名
	private static final byte[] columnFamily = Bytes
			.toBytes(MbConstants.XXDF_HBASE_FAMILY);

	// HBase客户端配置对象
	private static final Configuration conf = HBaseConfiguration.create();

	// HTable客户端对象
	private transient HTable hTable;

	// 扫描的日志起始时间戳，单位为秒，默认从当前时间的前6分钟时刻开始扫描
	private int startTimestamp = 0;

	// 扫描的日志结束时间戳，单位为秒，默认扫描到当前时间的前3分钟时刻为止
	private int endTimestamp = 0;

	// 记录最近扫描到的日志时间戳，单位为秒，默认为0
	private int lastTimestamp = 0;

	// 流量和业务日志的同步延迟时间，单位为秒，默认为0
	private int syncTsLatency = 0;
	
	private byte company = 2;   //天猫标识
	private byte is_shop = 1;   //店铺pv
	private byte url_type = 4;  //宝贝pv
	private byte [] ciu_suffix = new byte[3];   //扫描天猫宝贝流量日志的key 
	private byte endff  = (byte) 0xff;  //后缀

	/**
	 * 构造函数
	 * 
	 * @param tableName
	 */
	public NHbaseGenerator(String tableName) {
		this(tableName, 0, 0);
	}

	/**
	 * 构造函数
	 * 
	 * @param tableName
	 * @param startTimestamp
	 * @param endTimestamp
	 */
	public NHbaseGenerator(String tableName, int startTimestamp, int endTimestamp) {
		this.tableName = tableName;
		this.startTimestamp = startTimestamp;
		this.endTimestamp = endTimestamp;
		initCiuSuffix();
		validateTs();
		initHTable();
	}

	/**
	 * 设置流量和业务日志的同步延迟时间
	 * 
	 * @param syncTsLatency
	 */
	public void setSyncTsLatency(int syncTsLatency) {
		this.syncTsLatency = syncTsLatency;
	}

	/**
	 * 校验startTimestamp和endTimestamp的合法性
	 */
	private void validateTs() {
		int nowTimestamp = (int) (new DateTime().getMillis() / 1000);
		if (startTimestamp != 0
				&& startTimestamp < nowTimestamp
						- MbConstants.XXDF_HBASE_TIME_INTERVAL)
			throw new RuntimeException("start timestamp is more than "
					+ MbConstants.XXDF_HBASE_TIME_INTERVAL + " before");
		if (endTimestamp != 0
				&& endTimestamp > nowTimestamp
						+ MbConstants.XXDF_HBASE_TIME_INTERVAL)
			throw new RuntimeException("end timestamp is more than "
					+ MbConstants.XXDF_HBASE_TIME_INTERVAL + " after");
		if (startTimestamp != 0
				&& endTimestamp != 0
				&& startTimestamp + MbConstants.XXDF_HBASE_TIME_INTERVAL < endTimestamp)
			throw new RuntimeException(
					"[start timestamp, end timestamp] is bigger than "
							+ MbConstants.XXDF_HBASE_TIME_INTERVAL);
		this.lastTimestamp = this.startTimestamp;
	}

	/**
	 * 初始化客户端HTable对象，用户从HBase读取数据
	 * 
	 * @return
	 */
	@SuppressWarnings("deprecation")
	private boolean initHTable() {
		try {
			hTable = new HTable(conf, tableName);
			// 按目前淘宝全网流量日志峰值计算，预估每个Spout的每个分区处理线程每秒处理数据为1000~2000条，
			// 但是考虑到region server上的hbase.regionserver.lease.period默认配置为60秒
			// 超时，故客户端的scannerCaching暂时设置为500条！！！
			hTable.setScannerCaching(MbConstants.XXDF_HBASE_MAIN_TABLE_RECORD_NUM);
		} catch (IOException e) {
			LOG.error("initialize htable error, table name: " + tableName, e);
			return false;
		}
		return true;
	}

	/**
	 * 初始化客户端从HBase读取数据的数据rowkey后缀
	 * 
	 * @return
	 */
	private boolean initCiuSuffix() {
		
		Bytes.putByte(ciu_suffix, 0, company);
		Bytes.putByte(ciu_suffix, 1, is_shop);
		Bytes.putByte(ciu_suffix, 2, url_type);
		
		return true;
		
		
	}
	
	/**
	 * 根据当前时间戳，计算得到指定秒前的UNIX时间戳
	 * 
	 * @param secInterval
	 * @return
	 */
	private int makeTimestamp(int secInterval) {
		long now = new DateTime().getMillis();
		int nowTs = (int) (now / 1000);
		return (nowTs - secInterval);
	}

	/**
	 * 获取最近扫描到的日志时间戳，注意仅限于browse_log、ad_ex_clk、biz_order、pay_order、
	 * alipay_rtn_order
	 * 
	 * @return
	 */
	public int getLastTimestamp() {
		return lastTimestamp;
	}

	/**
	 * 扫描HBase表: browse_log表和biz_order表
	 * 
	 * @param shardingKey
	 * @param lastKey
	 * @return
	 */
	public ResultScanner scanMainTable(short shardingKey, byte[] lastKey) {
		ResultScanner rs = null;
		Scan scan = new Scan();
		scan.addFamily(columnFamily);
		int startTs = 0;
		int endTs = 0;
		byte[] startKey = lastKey;
		byte[] endKey = new byte[5];
		if (shardingKey >= 0 && shardingKey < MbConstants.XXDF_HBASE_SHARDING_NUM) { // 按照rowkey范围扫描
			byte bSharding = (byte) shardingKey;
			int startTsMode;
			if (startKey == null) { // 第一次查询时设置startTs和startKey
				startKey = new byte[5];
				if (startTimestamp == 0) {
					startTs = makeTimestamp(MbConstants.XXDF_HBASE_LOG_SCAN_END_SEC
							+ MbConstants.XXDF_HBASE_LOG_SCAN_MAX_SECS);
				} else {
					startTs = startTimestamp;
				}
				startTsMode = startTs % MbConstants.XXDF_HBASE_TIME_INTERVAL;
				Bytes.putByte(startKey, 0, bSharding);
				Bytes.putInt(startKey, 1, startTsMode);
			} else { // 否则根据startKey得到startTs（这个startTs是0~7*24*3600之间的ts）
				startTsMode = Bytes.toInt(startKey, 1, Bytes.SIZEOF_INT);
			}

			// 初始化设置endTs和endKey
			if (endTimestamp == 0) {
				endTs = makeTimestamp(MbConstants.XXDF_HBASE_LOG_SCAN_END_SEC);
			} else {
				endTs = endTimestamp;
			}
			// 设置业务日志同步延迟时间
			if (MbConstants.XXDF_HBASE_BIZORDER_TABLE.equals(tableName))
				endTs -= syncTsLatency;
			int endTsMode = endTs % MbConstants.XXDF_HBASE_TIME_INTERVAL;

			// XXX: 每次最多扫描3分钟时间间隔内的数据，以避免一次scanner设置过长时间进行扫描导致超时异常！
			final int maxScanSecs = MbConstants.XXDF_HBASE_LOG_SCAN_MAX_SECS;
			if ((endTsMode > startTsMode && endTsMode - startTsMode > maxScanSecs) // 处理[...startTsMode...endTsMode...]
					|| (endTsMode < startTsMode && MbConstants.XXDF_HBASE_TIME_INTERVAL
							- startTsMode + endTsMode > maxScanSecs)) { // 处理[...endTsMode...startTsMode...]
				endTsMode = (startTsMode + maxScanSecs)
						% MbConstants.XXDF_HBASE_TIME_INTERVAL;
				endTs = (lastKey == null) ? startTs + maxScanSecs
						: lastTimestamp + maxScanSecs;
			}
			// XXX: 根据startTsMode和endTsMode的大小关系分为以下两种情况进行处理
			if (endTsMode >= startTsMode) { // 处理[...startTsMode...endTsMode...]
				Bytes.putByte(endKey, 0, bSharding);
				Bytes.putInt(endKey, 1, endTsMode);
			} else { // 处理[...endTsMode...startTsMode...]
				Bytes.putByte(endKey, 0, bSharding);
				Bytes.putInt(endKey, 1, MbConstants.XXDF_HBASE_TIME_INTERVAL);
				// XXX: 发生时间轮转时将endTs置为上一次lastTimestamp之后的轮转时间的UNIX时间戳；
				// 由于每次发生轮转时会两次走到这个逻辑分支，第二次时lastTimestamp已为轮转时间的UNIX时间戳，因此不再更新endTs！
				// 此处修复了一个严重线上运行BUG，线下不可复现，改动这块代码之前请三思，确保真正理解了代码逻辑含义！！！
				endTs = (lastKey == null) ? startTs : lastTimestamp;
				if (endTs % MbConstants.XXDF_HBASE_TIME_INTERVAL != 0)
					endTs = (endTs / MbConstants.XXDF_HBASE_TIME_INTERVAL + 1)
							* MbConstants.XXDF_HBASE_TIME_INTERVAL;
				LOG.info("sharding=" + shardingKey
						+ ":log time rotating occured!");
				LOG.info("sharding=" + shardingKey + ",startTsMode="
						+ startTsMode + ",endTsMode=" + endTsMode + ",endTs="
						+ endTs + ",lastTimestamp=" + lastTimestamp);
			}
			assert (endTs >= lastTimestamp);

			if (Bytes.compareTo(startKey, endKey) > 0) { // startKey大于endKey的异常情况
				LOG.error("invalid parameters for table " + tableName + "("
						+ shardingKey + "): startKey is greater than endKey!");
				return null;
			} else if (Bytes.compareTo(startKey, endKey) == 0) { // 已扫描到endTimestamp的情况
				return null;
			}

			// 设置查询的rowkey范围
			scan.setStartRow(startKey);
			scan.setStopRow(endKey);
		} else { // 按照时间范围扫描（暂时不使用该种扫描方法！）
			startTs = makeTimestamp(MbConstants.XXDF_HBASE_LOG_SCAN_END_SEC
					+ MbConstants.XXDF_HBASE_LOG_SCAN_MAX_SECS);
			endTs = makeTimestamp(MbConstants.XXDF_HBASE_LOG_SCAN_END_SEC);
			long minStamp = startTimestamp != 0 ? (long) startTimestamp * 1000
					: (long) startTs * 1000;
			long maxStamp = endTimestamp != 0 ? (long) endTimestamp * 1000
					: (long) endTs * 1000;
			try {
				scan.setTimeRange(minStamp, maxStamp);
			} catch (IOException e) {
				LOG.error("set scan timerange error for table " + tableName
						+ "(" + shardingKey + ")", e);
				return null;
			}
		}

		try {
			rs = hTable.getScanner(scan);
		} catch (IOException e) {
			LOG.error("get htable scanner error for table " + tableName + "("
					+ shardingKey + ")", e);
			return null;
		}

		// 记录最近一次扫描到的时间戳
		lastTimestamp = endTs;
		return rs;
	}

	/**
	 * 扫描HBase表: biz_order_index表
	 * 
	 * @param shardingKey
	 * @param parentKey
	 * @return
	 */
	public ResultScanner scanIndexTable(short shardingKey, byte[] parentKey) {
		if (parentKey == null)
			return null;
		ResultScanner rs = null;
		Scan scan = new Scan();
		scan.addFamily(columnFamily);
		scan.setCaching(MbConstants.XXDF_HBASE_INDEX_TABLE_RECORD_NUM);

		if (shardingKey >= 0 && shardingKey < MbConstants.XXDF_HBASE_SHARDING_NUM) { // 按sharding
																					// key进行范围扫描
			byte[] shardKey = { (byte) shardingKey };
			byte[] startKey;
			byte[] endKey;

			startKey = new byte[45];
			startKey = Bytes.add(shardKey, parentKey,
					Bytes.toBytes("0000000000000000000000"));
			endKey = new byte[45];
			endKey = Bytes.add(shardKey, parentKey,
					Bytes.toBytes("9999999999999999999999"));
			scan.setStartRow(startKey);
			scan.setStopRow(endKey);
		} else { // 异常sharding key的情况
			LOG.error("invalid shardingKey key found");
			return null;
		}

		try {
			rs = hTable.getScanner(scan);
		} catch (IOException e) {
			LOG.error("get htable scanner error(" + shardingKey + ")", e);
			return null;
		}
		return rs;
	}

	/**
	 * 批量查询HBase表: biz_order表和pay_order表
	 * 
	 * @param keyList
	 * @return
	 */
	public Result[] getMainTable(List<byte[]> keyList) {
		List<Get> listGet = new ArrayList<Get>();
		for (byte[] key : keyList) {
			Get g = new Get(key);
			g.addFamily(columnFamily);
			if (!listGet.contains(g)) {
				listGet.add(g);
			}
		}
		if (listGet.size() == 0)
			return null;

		Result[] results = null;
		try {
			results = hTable.get(listGet);
		} catch (IOException e) {
			LOG.error("htable get list error", e);
		}

		return results;
	}

	/**
	 * 批量查询HBase表: pay_order_index表
	 * 
	 * @param orderIdList
	 * @return
	 */
	public Result[] getIndexTable(List<String> orderIdList) {
		List<Get> listGet = new ArrayList<Get>();
		for (String orderId : orderIdList) {
			byte[] bOrderIdKey = Bytes.toBytes(orderId);
			CRC32 crc32 = new CRC32();
			crc32.update(bOrderIdKey);
			byte shardingKey = (byte) (crc32.getValue() % 32);
			byte[] bShardingKey = { shardingKey };
			byte[] rowKey = Bytes.add(bShardingKey, bOrderIdKey);

			Get g = new Get(rowKey);
			g.addFamily(columnFamily);
			if (!listGet.contains(g)) {
				listGet.add(g);
			}
		}
		if (listGet.size() == 0)
			return null;

		Result[] results = null;
		try {
			results = hTable.get(listGet);
		} catch (IOException e) {
			LOG.error("htable get list error", e);
		}

		return results;
	}

	/**
	 * 扫描HBase表: browse_log表
	 * 
	 * @param shardingKey
	 * @param startTS - unixtimestamp
	 * @return
	 */
	public ResultScanner scanMainTableTs(short shardingKey,int timestamp) {
		ResultScanner rs = null;
		Scan scan = new Scan();
		scan.addFamily(columnFamily);
		int startTs = 0;
		int endTs = 0;
		byte[] startKey = new byte[8];
		byte[] endKey = new byte[9];
		if (shardingKey >= 0 && shardingKey < MbConstants.XXDF_HBASE_SHARDING_NUM) { // 按照rowkey范围扫描
			byte bSharding = (byte) shardingKey;
			int startTsMode;
			if (timestamp == 0) { // 第一次查询时设置startTs和startKey
				if (startTimestamp == 0) {
					startTs = makeTimestamp(MbConstants.XXDF_HBASE_LOG_SCAN_END_SEC
							+ MbConstants.XXDF_HBASE_LOG_SCAN_MAX_SECS);
				} else {
					startTs = startTimestamp;
				}

			} else { // 否则根据startKey得到startTs（这个startTs是0~7*24*3600之间的ts）
				startTs =  timestamp;
			}
            
			//防止扫描进程跟的太紧，丢失数据，默认跟到2分钟前
			int nowTs = makeTimestamp(MbConstants.XXDF_HBASE_LOG_SCAN_NOW_SEC);
			
			if(startTs >= nowTs){
				return null;
			}
			
			startTsMode = startTs % MbConstants.XXDF_HBASE_TIME_INTERVAL;
			Bytes.putByte(startKey, 0, bSharding);
			Bytes.putInt(startKey, 1, startTsMode);
			Bytes.putBytes(startKey, 5, ciu_suffix, 0, 3);
			
			
			
			// 初始化设置endTs
			endTs = startTs + 1;
			
			//判断指定的时间段，这时程序该退出
			if (endTimestamp != 0) {
	
				if(endTs > endTimestamp)
					return null;
			
			} 
			
			// 设置业务日志同步延迟时间
			if (MbConstants.XXDF_HBASE_BIZORDER_TABLE.equals(tableName))
				endTs -= syncTsLatency;
			int endTsMode = endTs % MbConstants.XXDF_HBASE_TIME_INTERVAL;

			// XXX: 每次最多扫描3分钟时间间隔内的数据，以避免一次scanner设置过长时间进行扫描导致超时异常！
			
			// xxx : 本次采用一秒一秒扫描，不会走到下面的分支
			final int maxScanSecs = MbConstants.XXDF_HBASE_LOG_SCAN_MAX_SECS;
			if ((endTsMode > startTsMode && endTsMode - startTsMode > maxScanSecs) // 处理[...startTsMode...endTsMode...]
					|| (endTsMode < startTsMode && MbConstants.XXDF_HBASE_TIME_INTERVAL
							- startTsMode + endTsMode > maxScanSecs)) { // 处理[...endTsMode...startTsMode...]
				endTsMode = (startTsMode + maxScanSecs)
						% MbConstants.XXDF_HBASE_TIME_INTERVAL;
				endTs = (timestamp == 0) ? startTs + maxScanSecs
						: lastTimestamp + maxScanSecs;
			}
			
			
			// XXX: 根据startTsMode和endTsMode的大小关系分为以下两种情况进行处理
			if (endTsMode >= startTsMode) { // 处理[...startTsMode...endTsMode...]
				Bytes.putByte(endKey, 0, bSharding);
				Bytes.putInt(endKey, 1, startTsMode);
				Bytes.putBytes(endKey, 5, ciu_suffix, 0, 3);
				Bytes.putByte(endKey, 8, endff);
				
			} else { // 处理[...endTsMode...startTsMode...]
				Bytes.putByte(endKey, 0, bSharding);
				Bytes.putInt(endKey, 1, startTsMode);
				Bytes.putBytes(endKey, 5, ciu_suffix, 0, 3);
				Bytes.putByte(endKey, 8, endff);
				
				/*
				// XXX: 发生时间轮转时将endTs置为上一次lastTimestamp之后的轮转时间的UNIX时间戳；
				// 由于每次发生轮转时会两次走到这个逻辑分支，第二次时lastTimestamp已为轮转时间的UNIX时间戳，因此不再更新endTs！
				// 此处修复了一个严重线上运行BUG，线下不可复现，改动这块代码之前请三思，确保真正理解了代码逻辑含义！！！
				endTs = (lastKey == null) ? startTs : lastTimestamp;
				if (endTs % Constants.XXDF_HBASE_TIME_INTERVAL != 0)
					endTs = (endTs / Constants.XXDF_HBASE_TIME_INTERVAL + 1)
							* Constants.XXDF_HBASE_TIME_INTERVAL;
				*/
				LOG.info("sharding=" + shardingKey
						+ ":log time rotating occured!");
				LOG.info("sharding=" + shardingKey + ",startTsMode="
						+ startTsMode + ",endTsMode=" + endTsMode + ",endTs="
						+ endTs + ",lastTimestamp=" + lastTimestamp);
			}
			assert (endTs >= lastTimestamp);

			if (Bytes.compareTo(startKey, endKey) > 0) { // startKey大于endKey的异常情况
				LOG.error("invalid parameters for table " + tableName + "("
						+ shardingKey + "): startKey is greater than endKey!");
				return null;
			} else if (Bytes.compareTo(startKey, endKey) == 0) { // 已扫描到endTimestamp的情况
				return null;
			}

			// 设置查询的rowkey范围
			scan.setStartRow(startKey);
			scan.setStopRow(endKey);
		} else { // 按照时间范围扫描（暂时不使用该种扫描方法！）
			startTs = makeTimestamp(MbConstants.XXDF_HBASE_LOG_SCAN_END_SEC
					+ MbConstants.XXDF_HBASE_LOG_SCAN_MAX_SECS);
			endTs = makeTimestamp(MbConstants.XXDF_HBASE_LOG_SCAN_END_SEC);
			long minStamp = startTimestamp != 0 ? (long) startTimestamp * 1000
					: (long) startTs * 1000;
			long maxStamp = endTimestamp != 0 ? (long) endTimestamp * 1000
					: (long) endTs * 1000;
			try {
				scan.setTimeRange(minStamp, maxStamp);
			} catch (IOException e) {
				LOG.error("set scan timerange error for table " + tableName
						+ "(" + shardingKey + ")", e);
				return null;
			}
		}

		try {
			rs = hTable.getScanner(scan);
		} catch (IOException e) {
			LOG.error("get htable scanner error for table " + tableName + "("
					+ shardingKey + ")", e);
			return null;
		}

		// 记录最近一次扫描到的时间戳
		lastTimestamp = endTs;
		return rs;
	}

	
	
}
