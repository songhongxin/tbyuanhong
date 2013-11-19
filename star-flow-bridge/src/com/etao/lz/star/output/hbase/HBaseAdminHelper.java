package com.etao.lz.star.output.hbase;

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
import org.apache.hadoop.hbase.util.Bytes;

import com.etao.lz.star.Tools;

public class HBaseAdminHelper {
	private byte[][] splits = new byte[][] { { (byte) 1 }, { (byte) 2 },
			{ (byte) 3 }, { (byte) 4 }, { (byte) 5 }, { (byte) 6 },
			{ (byte) 7 }, { (byte) 8 }, { (byte) 9 }, { (byte) 10 },
			{ (byte) 11 }, { (byte) 12 }, { (byte) 13 }, { (byte) 14 },
			{ (byte) 15 }, { (byte) 16 }, { (byte) 17 }, { (byte) 18 },
			{ (byte) 19 }, { (byte) 20 }, { (byte) 21 }, { (byte) 22 },
			{ (byte) 23 }, { (byte) 24 }, { (byte) 25 }, { (byte) 26 },
			{ (byte) 27 }, { (byte) 28 }, { (byte) 29 }, { (byte) 30 },
			{ (byte) 31 } };

	private Log LOG = LogFactory.getLog(HBaseAdminHelper.class);

	private Configuration HbaseConf;

	private int timeToLive;

	public HBaseAdminHelper(Configuration conf, int ttl) throws IOException {
		HbaseConf = conf;
		timeToLive = ttl;
	}

	public static void doHBaseConfig(Configuration hbaseConf, String zkCluster, String hbasePort, String zkParent) {
		hbaseConf.set("hbase.zookeeper.property.clientPort",hbasePort);
		
		hbaseConf.set("hbase.zookeeper.quorum", zkCluster);
		hbaseConf.set("hbase.client.pause", "100");
		hbaseConf.set("hbase.client.retries.number", "10");

		if (zkParent != null) {
			hbaseConf.set("zookeeper.znode.parent", zkParent);
		}
	}

	public static final int WEEK_DAYS = 7;
	public static final int HOUR_SECONDS = 3600;
	public static final int SHARD_NUM = 32;
	public static final int DAY_HOURS = 24;

	private byte[][] getSplitKeyForFlowLog() {
		final float[] splitInterval = { 2, 8, 9, 10, 10.5f, 11, 11.5f, 12, 13,
				14, 14.5f, 15, 15.5f, 16, 16.5f, 17, 17.5f, 18, 19, 19.5f, 20,
				20.5f, 21, 21.5f, 22, 23 };
		byte[][] splitKey = new byte[SHARD_NUM * WEEK_DAYS
				* splitInterval.length][];
		for (int i = 0; i < SHARD_NUM; i++) {
			for (int j = 0; j < WEEK_DAYS; j++) {
				for (int k = 0; k < splitInterval.length; k++) {
					int roundTs = (int) (splitInterval[k] * HOUR_SECONDS)
							+ HOUR_SECONDS * DAY_HOURS * j;
					byte[] bShard = { (byte) i };
					byte[] bRoundTs = Bytes.toBytes(roundTs);
					splitKey[i * WEEK_DAYS * splitInterval.length + j
							* splitInterval.length + k] = Bytes.add(bShard,
							bRoundTs);
				}
			}
		}
		return splitKey;
	}

	public boolean create(String tableName) {
		HBaseAdmin admin = null;
		try {
			admin = new HBaseAdmin(this.HbaseConf);
			if (admin.tableExists(tableName)) {
				return true;
			}

			HTableDescriptor tableDesc = new HTableDescriptor(tableName);
			HColumnDescriptor family = new HColumnDescriptor(
					HBaseTableHelper.COLUMN_FAMILY);
			family.setMaxVersions(1);
			family.setTimeToLive(timeToLive);
			family.setInMemory(true);
			family.setBloomFilterType(BloomType.ROW);
			family.setDataBlockEncoding(DataBlockEncoding.DIFF);

			family.setCompressionType(Algorithm.LZO);
			tableDesc.setMemStoreFlushSize(67108864);
			tableDesc.addFamily(family);
			if (tableName.equals("browse_log")) {
				admin.createTable(tableDesc, getSplitKeyForFlowLog());
			} else {
				admin.createTable(tableDesc, splits);
			}
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

	public void delete(String tableName) {
		HBaseAdmin admin = null;
		try {
			admin = new HBaseAdmin(HbaseConf);
			boolean bExists = admin.tableExists(tableName);
			if (!bExists) {
				LOG.info(tableName + " not Exists!!!");
				admin.close();
				return;
			}
		} catch (Exception e) {
			LOG.error(Tools.formatError("HBaseHelper Error", e));
			return;
		}

		try {
			admin.disableTable(tableName);
		} catch (Exception e) {
			LOG.error(Tools.formatError("HBaseHelper Error", e));
		}
		try {
			admin.deleteTable(tableName);
		} catch (Exception e) {
			LOG.error(Tools.formatError("HBaseHelper Error", e));
		}
		try {
			if (admin != null)
				admin.close();
		} catch (IOException e) {
		}
	}
}
