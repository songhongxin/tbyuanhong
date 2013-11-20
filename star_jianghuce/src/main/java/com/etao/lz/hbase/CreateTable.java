package com.etao.lz.hbase;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.regionserver.StoreFile.BloomType;


import com.etao.lz.star.Tools;
import com.etao.lz.storm.tools.Constants;

public class CreateTable {

	/*
	 * 用于创建水绕田园项目的hbase表
	 * 
	 * 
	 * */
	
	
	private byte[][] splits = new byte[][] { { (byte) 1 }, { (byte) 2 },
			{ (byte) 3 }, { (byte) 4 }, { (byte) 5 }, { (byte) 6 },
			{ (byte) 7 }, { (byte) 8 }, { (byte) 9 }, { (byte) 10 },
			{ (byte) 11 }, { (byte) 12 }, { (byte) 13 }, { (byte) 14 },
			{ (byte) 15 }, { (byte) 16 }, { (byte) 17 }, { (byte) 18 },
			{ (byte) 19 }, { (byte) 20 }, { (byte) 21 }, { (byte) 22 },
			{ (byte) 23 }, { (byte) 24 }, { (byte) 25 }, { (byte) 26 },
			{ (byte) 27 }, { (byte) 28 }, { (byte) 29 }, { (byte) 30 },
			{ (byte) 31 } };

	private Log LOG = LogFactory.getLog(CreateTable.class);

	private Configuration HbaseConf;

	public CreateTable(Configuration conf) throws IOException {
		HbaseConf = conf;
	}

	public boolean create(String tableName) {
		HBaseAdmin admin = null;
		try {
			admin = new HBaseAdmin(this.HbaseConf);
			if (admin.tableExists(tableName)) {
				System.out.println("Table :" + tableName
						+ "has been created !!!");
				return true;
			}

			HTableDescriptor tableDesc = new HTableDescriptor(tableName);
			HColumnDescriptor family = new HColumnDescriptor(
					Constants.BIGB_HBASE_FAMILY);
			family.setMaxVersions(1);
			family.setTimeToLive(Constants.BIGB_HBASE_TIME_INTERVAL);
			family.setInMemory(true);
			family.setBloomFilterType(BloomType.ROW);
			family.setDataBlockEncoding(DataBlockEncoding.DIFF);

			family.setCompressionType(Algorithm.LZO);
			tableDesc.setMemStoreFlushSize(67108864);
			tableDesc.addFamily(family);

			admin.createTable(tableDesc, splits);
			return true;
		} catch (Exception e) {
			LOG.error(Tools.formatError("HBaseHelper Error", e));
			return false;
		} finally {
			try {
				if (admin != null)
					admin.close();
			} catch (IOException e) {
			}
		}

	}

/*
	public static void main(String[] args) throws IOException {

		Configuration conf = HBaseConfiguration.create();
		
		System.out.println(conf.get("zk_servers"));
	
		CreateTable C = new CreateTable(conf);

		System.out.println("start  create  table ================>");

		if (C.create(Constants.BIGB_DP_B_TABLE)) {

			System.out.println("start  create  table   : "
					+ Constants.BIGB_DP_B_TABLE + "done ");

		}

		if (C.create(Constants.BIGB_DP_T_TABLE)) {

			System.out.println("start  create  table   : "
					+ Constants.BIGB_DP_T_TABLE + "done ");

		}

		if (C.create(Constants.FLOW_DP_B_TABLE)) {

			System.out.println("start  create  table   : "
					+ Constants.FLOW_DP_B_TABLE + "done ");

		}

		if (C.create(Constants.FLOW_DP_T_TABLE)) {

			System.out.println("start  create  table   : "
					+ Constants.FLOW_DP_T_TABLE + "done ");
			
		}

	}
*/
}