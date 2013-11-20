package com.etao.lz.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.etao.lz.storm.tools.Constants;

public class HbaseETL {

	private static final byte[] columnFamily = Bytes
			.toBytes(Constants.BIGB_HBASE_FAMILY);

	public static final String tablenameD = Constants.DP_D_TABLE;
	public static final String tablenameH = Constants.DP_H_TABLE;

	static Configuration conf;

	/*
	 * 查看表里指定条件的数目 参数1，mark@string，"d"/"h",查看天表或者小时表 参数2，date，"20131102",
	 * 参数3，timeinterval，"0-23"
	 */
	public static void countTable(String mark, int date, byte timeinterval)
			throws IOException {

		Configuration cconf = new Configuration();
		cconf.addResource("hbase-site-new.xml");

		conf = HBaseConfiguration.create(cconf);
		HBaseConfiguration.addHbaseResources(conf);

		HTable hTableD = new HTable(conf, tablenameD);
		HTable hTableH = new HTable(conf, tablenameH);

		try {
			Scan scan = new Scan();
			scan.addFamily(columnFamily);
			scan.setCaching(10000);

			ResultScanner scanner;
			if (mark.equals("d")) {
				scanner = hTableD.getScanner(scan);
			} else {
				scanner = hTableH.getScanner(scan);
			}

			byte[] d = Bytes.toBytes(date);

			boolean flag = true;
			long count = 0;

			if (mark.equals("d")) {
				for (Result res : scanner) {
					byte[] row = res.getRow();
					if (row.length == 21) {
						for (int i = 9, j = 0; j < 4; i++, j++) {
							if (d[j] != row[i]) {
								flag = false;
								break;
							}
						}
						if (flag == true) {
							count++;
						}

					}
				}
				System.out.println("This date count in hbase is " + count);
			} else {
				for (Result res : scanner) {
					byte[] row = res.getRow();
					if (row.length == 22) {
						for (int i = 9, j = 0; j < 4; i++, j++) {
							if (d[j] != row[i]) {
								flag = false;
								break;
							}
						}
						if (timeinterval != row[13]) {
							flag = false;
							break;
						}
						if (flag == true) {
							count++;
						}

					}
				}
				System.out.println("This date this hour count in hbase is "
						+ count);

			}
			scanner.close();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/*
	 * 清楚天表指定日期所有数据 参数1、"int 20131102"
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void cleanTable(String mark, int date, byte timeinterval)
			throws IOException {

		Configuration cconf = new Configuration();
		cconf.addResource("hbase-site-new.xml");

		conf = HBaseConfiguration.create(cconf);
		HBaseConfiguration.addHbaseResources(conf);

		HTable hTableD = new HTable(conf, tablenameD);
		HTable hTableH = new HTable(conf, tablenameH);

		try {
			Scan scan = new Scan();
			scan.addFamily(columnFamily);
			scan.setCaching(10000);

			ResultScanner scanner;
			if (mark.equals("d")) {
				scanner = hTableD.getScanner(scan);
			} else {
				scanner = hTableH.getScanner(scan);
			}

			List list = new ArrayList();
			byte[] d = Bytes.toBytes(date);
			boolean flag = true;

			if (mark.equals("d")) {
				for (Result res : scanner) {
					byte[] row = res.getRow();
					if (row.length == 21) {
						for (int i = 9, j = 0; j < 4; i++, j++) {
							if (d[j] != row[i]) {
								flag = false;
								break;
							}
						}
						if (flag == true) {
							Delete delete = new Delete(row);
							list.add(delete);
							if (list.size() == 5000) {
								hTableH.delete(list);
								list = null;
								list = new ArrayList();
							}
						}

					}
				}
			} else {
				for (Result res : scanner) {
					byte[] row = res.getRow();
					if (row.length == 22) {
						for (int i = 9, j = 0; j < 4; i++, j++) {
							if (d[j] != row[i]) {
								flag = false;
								break;
							}
						}
						if (timeinterval != row[13]) {
							flag = false;
							break;
						}
						if (flag == true) {
							Delete delete = new Delete(row);
							list.add(delete);
							if (list.size() == 5000) {
								hTableH.delete(list);
								list = null;
								list = new ArrayList();
							}
						}

					}
				}

				hTableH.delete(list);

			}
			scanner.close();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws IOException {

		if (args.length < 3) {
			System.out
					.println("please input mark(d/h), date(20131102), timeinterval(0-23) !");
			System.exit(0);
		}

		String mark = args[0];
		int date = Integer.parseInt(args[1]);

		long bts = System.currentTimeMillis();
		byte t = 0;

		if (mark.equals("d")) {
			System.out.println("查询天表!");
			HbaseETL.countTable("d", date, t);
			HbaseETL.cleanTable("d", date, t);
		} else {
			int ot = Integer.parseInt(args[2]);
			t = (byte) ot;

			System.out.println("查询小时表!");
			HbaseETL.countTable("h", date, t);
		}

		long ats = System.currentTimeMillis();

		long pts = (ats - bts) / 1000;

		System.out.println("The time count uses is " + pts);

		/*
		 * System.out.println(System.currentTimeMillis());
		 * System.out.println(TimeUtils.getCurrMinTime());
		 * System.out.println(TimeUtils.getTimeHour(1381477560L));
		 * System.out.println(Byte.parseByte("11"));
		 * BigbReaderCheck.readUnit(sellerid, ts);
		 */
	}

}
