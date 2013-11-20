package com.etao.lz.hbase;

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
import org.apache.hadoop.hbase.util.Bytes;

import com.etao.lz.storm.tools.Constants;




/*
 * 吸星大法虫洞hbase 数据查询类
 * 
 * 关联成交订单索引表，成交订单表
 * 
 * 参数  ：  order_id 
 * 
 * by  yuanhong.shx
 * 
 * */
public class HBaseGenerator implements java.io.Serializable{

	/**
	 *id  
	 */
	private static final long serialVersionUID = -8953314526475115520L;
	
	// 日志操作记录对象
	private static final Log LOG = LogFactory.getLog(HBaseGenerator.class);

	// 吸星大法HBase表名
	private String tableName;

	// 吸星大法HBase表列簇名
	private static final byte[] columnFamily = Bytes
			.toBytes(Constants.BIGB_HBASE_FAMILY);

	// HBase客户端配置对象
	private static final Configuration conf = HBaseConfiguration.create();

	// HTable客户端对象
	private transient HTable hTable;
	
	/**
	 * 构造函数
	 * 
	 * @param tableName
	 */
	public HBaseGenerator(String tableName) {
		this.tableName = tableName;
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
		//	hTable.setScannerCaching(Constants.XXDF_HBASE_MAIN_TABLE_RECORD_NUM);
		} catch (IOException e) {
			LOG.error("initialize htable error, table name: " + tableName, e);
			return false;
		}
		return true;
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
	 * 单个查询HBase表: biz_order表和pay_order表
	 * 
	 * @param key
	 * @return
	 */
	public Result getMainTable(byte[] key) {

			Get g = new Get(key);
			g.addFamily(columnFamily);

	
		Result result = null;
		try {
			result = hTable.get(g);
		} catch (IOException e) {
			LOG.error("htable get  error", e);
		}

		return result;
	}

	/**
	 * 单个查询HBase表: pay_order_index表
	 * 
	 * @param orderIdList
	 * @return
	 */
	public Result getIndexTable(String orderId) {

		byte[] bOrderIdKey = Bytes.toBytes(orderId);
		CRC32 crc32 = new CRC32();
		crc32.update(bOrderIdKey);
		byte shardingKey = (byte) (crc32.getValue() % 32);
		byte[] bShardingKey = { shardingKey };
		byte[] rowKey = Bytes.add(bShardingKey, bOrderIdKey);

		Get g = new Get(rowKey);
		g.addFamily(columnFamily);

		Result result = null;
		try {
			result = hTable.get(g);
		} catch (IOException e) {
			LOG.error("htable get error", e);
		}

		return result;
	}


}
