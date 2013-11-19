package com.etao.lz.star.output.hbase;

import java.io.IOException;
import java.util.Calendar;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.etao.lz.star.Tools;
import com.google.protobuf.GeneratedMessage;

public class HBaseTableHelper implements OutputHelper {
	public static final int SHARDING_KEY_NUM = 32;

	public static final byte[] COLUMN_FAMILY = Bytes.toBytes("d");

	public static final byte[] COLUMN = Bytes.toBytes("pb");

	private Log LOG = LogFactory.getLog(HBaseTableHelper.class);

	private HTable table = null;
	private String tableName;


	public HBaseTableHelper(String tablename, Configuration conf, long writeBufferSize)
			throws IOException {
		this.tableName = tablename;

		table = new HTable(conf, tableName);

		if (writeBufferSize != 0) {
			table.setAutoFlush(false, false);
			table.setWriteBufferSize(writeBufferSize);
		}
	}

	public void flush() throws IOException {
		table.flushCommits();
	}

	public void write(byte[] rowKey, byte[] data, GeneratedMessage log)
			throws IOException {

		Put p = new Put(rowKey);
		p.setWriteToWAL(true);

		p.add(COLUMN_FAMILY, COLUMN, data);
		IOException laste = null;
		for (int i = 0; i < 3; ++i) {
			try {
				table.put(p);
				return;
			} catch (IOException e) {
				LOG.error(Tools.formatError("HBaseTableHelper write Error", e));
				laste = e;
			}
		}
		if (laste != null) {
			throw laste;
		}
	}

	public ResultScanner scanLatestRecords(int seconds) throws IOException {
		Scan scan = new Scan();

		if (seconds > 0) {
			Calendar cal = Calendar.getInstance();
			long end = cal.getTimeInMillis();
			cal.add(Calendar.SECOND, -seconds);
			long start = cal.getTimeInMillis();
			scan.setTimeRange(start, end);
			LOG.info(String.format("scan last %d seconds", seconds));
		} else {
			LOG.info("scan full");
		}
		scan.addFamily(COLUMN_FAMILY);
		return table.getScanner(scan);
	}

	public ResultScanner scanTimeRange(long startTs, long endTs)
			throws IOException {
		Scan scan = new Scan();
		if (startTs > 0) {
			if (endTs <= 0) {
				endTs = Calendar.getInstance().getTimeInMillis();
			}
			LOG.info(String.format("scan timestamp range [%d, %d]", startTs,
					endTs));
			scan.setTimeRange(startTs, endTs);
		} else {
			LOG.info("scan full");
		}
		scan.addFamily(COLUMN_FAMILY);
		return table.getScanner(scan);
	}

	public ResultScanner scanTsRange(byte shardingKey, int start, int end)
			throws IOException {
		int n = HBaseAdminHelper.WEEK_DAYS * HBaseAdminHelper.DAY_HOURS
				* HBaseAdminHelper.HOUR_SECONDS;
		Scan scan = new Scan();
		scan.addFamily(COLUMN_FAMILY);

		byte[] startKey = new byte[5];
		byte[] endKey = new byte[5];
		if (shardingKey < 0 || shardingKey > SHARDING_KEY_NUM) {
			return null;
		}

		start = start % n;
		end = adjustEnd(start, end) % n;
		if(end == 0) end = n;
		Bytes.putByte(startKey, 0, shardingKey);
		Bytes.putInt(startKey, 1, start);

		Bytes.putByte(endKey, 0, shardingKey);
		Bytes.putInt(endKey, 1, end);

		LOG.info("scanTsRange:[" + start + ", " + end + ")");
		scan.setStartRow(startKey);
		scan.setStopRow(endKey);

		return table.getScanner(scan);
	}

	public String getTableName() {
		return tableName;
	}

	public static int adjustEnd(int start, int end) {
		int n = HBaseAdminHelper.WEEK_DAYS * HBaseAdminHelper.DAY_HOURS
				* HBaseAdminHelper.HOUR_SECONDS;
		int key_start = (int) (start % n);
		int key_end = (int) (end % n);
		if(key_end != 0 && key_end < key_start)
		{
			return end / n * n;
		}
		else
		{
			return end;
		}
	}
}
