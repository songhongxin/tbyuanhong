package com.etao.lz.storm.tools;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class DeteHbaseTool {

	// 吸星大法HBase表列簇名
	private static final byte[] columnFamily = Bytes
			.toBytes(Constants.XXDF_HBASE_FAMILY);

	/**
	 * 负责单个 shard的扫描和发送操作
	 * 
	 */
	// 日志操作记录对象

	public class ShardScanner implements Runnable {

		// public HbaseManager hbaseD;
		// public HbaseManager hbaseH;

		public Log LOG = LogFactory.getLog(ShardScanner.class);
		public short shardingKey;

		// HBase客户端配置对象
		public transient Configuration conf;

		// HTable客户端对象
		public transient HTable hTable;
		// HTable客户端对象
		public transient HTable dTable;

		public String hour = "";

		@SuppressWarnings("deprecation")
		public ShardScanner(short shardingKey, String table, String hour) {
			this.shardingKey = shardingKey;
			this.hour = hour;

			// 单元测试时，hbase需要注释掉
			Configuration cconf = new Configuration();
			cconf.addResource("hbase-site-new.xml");
			conf = HBaseConfiguration.create(cconf);

			try {
				hTable = new HTable(conf, table);
				hTable.setScannerCaching(1000);
				hTable.setAutoFlush(false);

				dTable = new HTable(conf, table);
				// dTable.setScannerCaching(300);
				dTable.setAutoFlush(false);

			} catch (Exception e) {
				LOG.error("initialize htable error, table name: "
						+ Constants.DP_D_TABLE, e);

			}

			// this.hbaseD = new HbaseManager(Constants.DP_D_TABLE);
			// this.hbaseH = new HbaseManager(Constants.DP_H_TABLE);

		}

		@Override
		public void run() {
			long totalCount = 0L;
			System.out.println(shardingKey + "   run  !!!");
			// Step 1) 按照shardingKey，从上次扫描截至的lastRowKey开始扫描日志
			short s = (short) ((shardingKey - 1) * 8);
			short e = (short) (shardingKey * 8);
			ResultScanner rs = scanD(s, e);
			totalCount = extract(rs, totalCount, hour);

		}

		/**
		 * 扫描HBase表: browse_log表
		 * 
		 * @param shardingKey
		 * @param startTS
		 *            - unixtimestamp
		 * @return
		 */
		public ResultScanner scanD(short shardingKey, short shardingEndkey) {
			ResultScanner rs = null;
			Scan scan = new Scan();
			scan.addFamily(columnFamily);

			byte[] startKey = new byte[1];
			byte[] endKey = new byte[1];
			byte sbSharding = (byte) shardingKey;
			byte ebSharding = (byte) shardingEndkey;
			Bytes.putByte(startKey, 0, sbSharding);
			Bytes.putByte(endKey, 0, ebSharding);

			// 设置查询的rowkey范围
			scan.setStartRow(startKey);
			scan.setStopRow(endKey);

			try {
				// System.out.println("look up  for  delet  record!!");
				rs = hTable.getScanner(scan);
			} catch (Exception e) {
				LOG.error("get hbase table  failed !!");
				return null;
			}

			// System.out.println("look up  for  delet  record!!");
			return rs;

		}

		public long extract(ResultScanner rs, long totalCount, String hour) {
			Result rr = new Result();

			ArrayList<Delete> dlist = new ArrayList<Delete>();
			byte[] rowkey = null;
			if (rs != null) { // XXX: 获取Scanner成功，遍历处理

				// System.out.println("while !!");
				while (rr != null) {

					try {
						rr = rs.next();
					} catch (Exception e) {
						short start = (short) ((shardingKey - 1) * 8);
						short end = (short) (shardingKey * 8);
						rs = scanD(start, end);
						System.out.println("exception  to  restart scan!");
						e.printStackTrace();
					}

					if (rr == null || rr.isEmpty())
						continue;
					rowkey = rr.getRow();

					if (!hour.equals("")) {

						// 过滤出该小时的数据
						short h = rowkey[1 + 8 + 4];

						if (!hour.equals(String.valueOf(h))) {

							continue;
						}

					}

					Delete d = new Delete(rowkey);
					totalCount++;
					dlist.add(d);

					if (totalCount % 10000 == 0) {

						try {

							LOG.info("delete  : " + dlist.size());
							LOG.info("totalcount :" + totalCount);
							dTable.delete(dlist);
						} catch (IOException e) {

							e.printStackTrace();

						}
						dlist = null;
						dlist = new ArrayList<Delete>();

					}

				}
				rs.close();
				System.out.println("scan over!");
			} else {

				System.out.println("ju ran  null lllllllllllllllllllllll");

			}
			return totalCount;
		}

	}

	public void start(String dh, String hour) {

		if (dh.equals("hour")) {

			try {
				for (short i = 1; i < 33; i++) {

					Thread scanThread = new Thread(new ShardScanner(i,
							Constants.DP_H_TABLE, hour));
					// scanThread.setDaemon(true);
					scanThread.start();

				}
			} catch (Exception e) {
				// TODO Auto-generated catch block

				System.out.println("create  delet  shard  failed !!!");
				e.printStackTrace();
			}

		} else if (dh.equals("day")) {

			// 删除天表
			try {
				for (short i = 1; i < 33; i++) {

					Thread scanThread = new Thread(new ShardScanner(i,
							Constants.DP_D_TABLE, hour));
					// scanThread.setDaemon(true);
					scanThread.start();

				}
			} catch (Exception e) {
				// TODO Auto-generated catch block

				System.out.println("create  delet  shard  failed !!!");
				e.printStackTrace();
			}

		}

	}

	public static void main(String[] args) {

		if (args.length < 1) {

			System.out
					.println("please put  day  or  hour ,hour  need  hourdex");
			System.out.println("ru  :  day  3");
			System.exit(0);
		}
		DeteHbaseTool d = new DeteHbaseTool();
		String hour = "";
		String dh = "";

		dh = args[0];

		if (dh.equals("day") || dh.equals("hour")) {

			if (args.length > 1) {
				hour = args[1];
			}
			d.start(dh, hour);

		} else {

			System.out.println("please put  right day  or  hour!!");

		}
		System.out.println("starting  delete   hbase--------------");

	}

}