package com.etao.lz.star;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.util.Iterator;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import com.etao.lz.star.output.hbase.HBaseAdminHelper;
import com.etao.lz.star.output.hbase.HBaseTableHelper;
import com.google.protobuf.InvalidProtocolBufferException;

public class HBaseTable {

	public static void outputIndex(String path, Iterator<Result> iter)
			throws IOException {
		OutputStreamWriter writer = new OutputStreamWriter(
				new FileOutputStream(path, false), "utf-8");
		try {
			while (iter.hasNext()) {

				Result next = iter.next();
				KeyValue kv = next
						.getColumnLatest(HBaseTableHelper.COLUMN_FAMILY,
								HBaseTableHelper.COLUMN);

				writer.write("RowKey:" + Tools.bytesToString(kv.getRow())
						+ "\n");

				writer.write("Value:" + Tools.bytesToString(kv.getValue())
						+ "\n");

			}
		} finally {
			writer.close();
		}
	}

	public static void output(String path, Iterator<Result> iter, String type)
			throws Exception {
		OutputStreamWriter writer = new OutputStreamWriter(
				new FileOutputStream(path), "utf-8");
		try {
			while (iter.hasNext()) {

				Result next = iter.next();
				KeyValue kv = next
						.getColumnLatest(HBaseTableHelper.COLUMN_FAMILY,
								HBaseTableHelper.COLUMN);

				writer.write("RowKey:" + Tools.bytesToString(kv.getRow())
						+ "\n");

				if (type.equals("flow")) {
					writer.write(StarLogProtos.FlowStarLog.parseFrom(
							kv.getValue()).toString());
				} else {
					writer.write(StarLogProtos.BusinessStarLog.parseFrom(
							kv.getValue()).toString());
				}
				writer.write("\n");
			}
		} catch (InvalidProtocolBufferException e) {
			throw new IOException(e);
		} finally {
			writer.close();
		}
	}

	public static void main(String[] args) throws Exception {
		Options opt = new Options();
		opt.addOption("create", true, "Create hbase table");
		opt.addOption("delete", true, "Delete hbase table");
		opt.addOption("scan", true, "Scan hbase table");
		opt.addOption("output", true, "Save scan result to file");
		opt.addOption("timerange", true, "Scan a timerange");
		opt.addOption("logtype", true, "Type of log");
		opt.addOption("timestart", true, "Start of time");
		opt.addOption("timeend", true, "End of time");
		opt.addOption("sharding", true, "Flow sharding");

		CommandLine cmd = new GnuParser().parse(opt, args);
		Properties prop = new Properties();
		InputStream in = HBaseTable.class
				.getResourceAsStream("/hbase.properties");
		prop.load(in);

		Configuration conf = HBaseConfiguration.create();
		HBaseAdminHelper.doHBaseConfig(conf, prop.getProperty("zkcluster"),
				prop.getProperty("hbaseport"), prop.getProperty("zkparent"));
		int ttl = Integer.parseInt(prop.getProperty("timeToLive"));
		HBaseAdminHelper helper = new HBaseAdminHelper(conf, ttl);

		if (cmd.hasOption("create")) {
			helper.create(cmd.getOptionValue("create"));
		} else if (cmd.hasOption("delete")) {
			helper.delete(cmd.getOptionValue("delete"));
		} else if (cmd.hasOption("scan")) {
			scan(cmd, conf, Long.parseLong(prop.getProperty("writeBufferSize")));
		} else {
			printHelp();
		}
	}

	private static void scan(CommandLine cmd, Configuration conf,
			long writeBufferSize) throws IOException, Exception {

		HBaseTableHelper table = new HBaseTableHelper(
				cmd.getOptionValue("scan"), conf, writeBufferSize);
		ResultScanner scan = null;
		if (cmd.hasOption("timerange")) {
			int time = Integer.parseInt(cmd.getOptionValue("timerange"));
			scan = table.scanLatestRecords(time);
		} else if (cmd.hasOption("sharding")) {
			byte b = Byte.parseByte(cmd.getOptionValue("sharding"));
			String start = cmd.getOptionValue("timestart");
			String end = cmd.getOptionValue("timeend");

			long startTs = start != null ? Tools.parseSimpleTime(start) : 0;
			long endTs = end != null ? Tools.parseSimpleTime(end) : 0;
			scan = table.scanTsRange(b, (int) (startTs / 1000),
					(int) (endTs / 1000));
		} else if (cmd.hasOption("timestart")) {

			String start = cmd.getOptionValue("timestart");
			String end = cmd.getOptionValue("timeend");

			long startTs = start != null ? Tools.parseSimpleTime(start) : 0;
			long endTs = end != null ? Tools.parseSimpleTime(end) : 0;

			scan = table.scanTimeRange(startTs, endTs);
		}

		String type = "business";
		if (cmd.hasOption("logtype")) {
			type = cmd.getOptionValue("logtype");
		}
		if (cmd.hasOption("output")) {
			output(cmd.getOptionValue("output"), scan.iterator(), type);
		} else {

			print(scan.iterator(), type);
		}
		scan.close();
	}

	private static void print(Iterator<Result> iter, String type) {

		while (iter.hasNext()) {

			Result next = iter.next();
			KeyValue kv = next.getColumnLatest(HBaseTableHelper.COLUMN_FAMILY,
					HBaseTableHelper.COLUMN);

			System.out.println("Timestamp:"
					+ Tools.formatTime(kv.getTimestamp()));
			try {
				System.out
						.println("RowKey:" + Tools.bytesToString(kv.getRow()));

				if (type.equals("flow")) {
					System.out.println(StarLogProtos.FlowStarLog.parseFrom(kv
							.getValue()));
				} else {
					System.out.println(StarLogProtos.BusinessStarLog
							.parseFrom(kv.getValue()));
				}
			} catch (InvalidProtocolBufferException ex) {
				System.out.println(next);
			}
		}

	}

	private static void printHelp() {
		System.out.println("\nUsage:");
		System.out.println("    HBaseTable -create table-name");
		System.out.println("    HBaseTable -delete table-name\n");
		System.out.println("    HBaseTable -scan table-name \n");
		System.out.println("         [-logtype flow|business] ");
		System.out.println("         [-output path] \n");
		System.out.println("         [-timerange seconds] \n");
		System.out
				.println("         [-timestart yyyyMMddHHmmss] [-timeend yyyyMMddHHmmss]\n");
		System.out
				.println("         [-sharding shard_no] [-timestart yyyyMMddHHmmss] [-timeend yyyyMMddHHmmss]\n");
	}
	
}
