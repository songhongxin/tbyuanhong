package com.etao.lz.star;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Throwables;
import com.google.common.io.ByteStreams;
import com.google.common.io.CharStreams;
import com.google.protobuf.Descriptors.Descriptor;

public class Tools {
	public static final String PROCESS_START = "Start";
	public static final String PROCESS_VM_RSS = "VmRss";
	public static final String PROCESS_CPU = "CPU";
	private static Log LOG = LogFactory.getLog(Tools.class);
	private static RuntimeMXBean mxbean = ManagementFactory.getRuntimeMXBean();

	public static String bytesToString(byte[] bs) {
		if (bs == null)
			return null;
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < bs.length; ++i) {
			sb.append(String.format("%d", bs[i]));
			sb.append(',');
		}
		return sb.toString();
	}

	public static void main(String[] args) {
		System.out.println(-1 >> 16);
		System.out.println(-1 >>> 16);
	}

	public static String getIp(long longIP) {
		StringBuffer sb = new StringBuffer("");
		sb.append(String.valueOf((long) (longIP & 0x000000FF)));
		sb.append(".");
		sb.append(String.valueOf((long) (longIP & 0x0000FFFF) >> 8));
		sb.append(".");
		sb.append(String.valueOf((long) (longIP & 0x00FFFFFF) >> 16));
		sb.append(".");
		long tmpLong = (long) longIP >> 24;
		if (tmpLong < 0)
			tmpLong += 256;
		sb.append(String.valueOf(tmpLong));
		return sb.toString();
	}

	public static String formatError(String info, Throwable e) {
		return String.format("[Star-Storm]%s:%s", info,
				Throwables.getStackTraceAsString(e));
	}

	public static String join(String[] s) {
		if (s == null)
			return null;
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < s.length; ++i) {
			sb.append(s[i]);
			if (i + 1 < s.length) {
				sb.append(",");
			}
		}
		return sb.toString();
	}

	public static String join(Collection<String> ss) {
		if (ss == null)
			return null;
		if (ss.isEmpty())
			return "";
		StringBuilder sb = new StringBuilder();
		for (String s : ss) {
			sb.append(s);
			sb.append(",");
		}
		return sb.substring(0, sb.length() - 1);
	}

	public static void deleteDir(File dir) {
		for (File f : dir.listFiles()) {
			if (f.isDirectory()) {
				deleteDir(f);
			} else {
				f.delete();
			}
		}
	}

	public static Descriptor getDescriptor(String logClass) {
		if (logClass.equals("flow")) {
			return StarLogProtos.FlowStarLog.getDescriptor();
		} else {
			return StarLogProtos.BusinessStarLog.getDescriptor();
		}
	}

	public static List<byte[]> split(final byte[] bytes, final byte delimiter) {
		final List<byte[]> bytesList = new ArrayList<byte[]>();
		int offset = 0;
		for (int i = 0; i <= bytes.length; i++) {
			if (i == bytes.length || bytes[i] == delimiter) {
				byte[] buffer = new byte[i - offset];
				System.arraycopy(bytes, offset, buffer, 0, buffer.length);
				offset = i + 1;
				bytesList.add(buffer);
			}
		}
		return bytesList;
	}

	private static ThreadLocal<SimpleDateFormat> simpleDateFormatLocal = new ThreadLocal<SimpleDateFormat>() {
		@Override
		protected SimpleDateFormat initialValue() {
			return new SimpleDateFormat("yyyyMMddHHmmss");
		}
	};

	private static ThreadLocal<SimpleDateFormat> goodDateFormatLocal = new ThreadLocal<SimpleDateFormat>() {
		@Override
		protected SimpleDateFormat initialValue() {
			return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		}
	};

	public static Map<String, String> parseQuery(String query) {
		HashMap<String, String> map = new HashMap<String, String>();
		int pos = query.indexOf('?');
		if (pos < 0)
			return map;
		query = query.substring(pos + 1);
		String[] params = query.split("&");

		for (String s : params) {
			String[] split = s.split("=");
			if (split.length != 2)
				continue;
			map.put(split[0].trim(), split[1].trim());
		}
		return map;
	}

	public static long parseSimpleTime(String s) {
		try {
			return simpleDateFormatLocal.get().parse(s).getTime();
		} catch (Throwable e) {
			LOG.error(Tools.formatError("Wrong Time Format", e));
			return 0;
		}
	}

	public static long parseGoodTime(String s) {
		try {
			return goodDateFormatLocal.get().parse(s).getTime();
		} catch (Throwable e) {
			LOG.error(Tools.formatError("Wrong Time Format", e));
			return 0;
		}
	}

	public static String formatTime(long ts) {
		return goodDateFormatLocal.get().format(new Date(ts));
	}

	public static String formatSimpleTime(long ts) {
		return simpleDateFormatLocal.get().format(new Date(ts));
	}

	public static String getIdAndHost() {
		return mxbean.getName();
	}

	public static String getHost() {
		String s = mxbean.getName();
		int pos = s.indexOf('@');
		if (pos <= 0) {
			throw new IllegalStateException("Could not extract hostname from "
					+ s);
		}
		return s.substring(pos + 1);
	}

	public static String getPId() {
		String id_host = mxbean.getName();
		int pos = id_host.indexOf('@');
		return id_host.substring(0, pos).trim();
	}

	public static String getListenPort(InputStream ins, String pid) {
		try {
			List<String> lines = CharStreams.readLines(new InputStreamReader(
					ins, "utf-8"));

			for (String line : lines) {
				String[] split = line.split("\\s+");
				if (split.length < 7)
					continue;
				if (!split[5].equals("LISTEN"))
					continue;
				if (!split[6].equals(String.format("%s/java", pid)))
					continue;
				int pos = split[3].indexOf(':');
				if (pos <= 0)
					continue;
				try {
					String value = split[3].substring(pos + 1);
					int n = Integer.parseInt(value);
					if (n < 6000 || n > 7000)
						continue;
					return value;
				} catch (Exception e) {
					continue;
				}
			}
		} catch (IOException e) {
			return "Unknown";
		}
		return "Unknown";
	}

	public static void getProcessInfo(Map<String, String> map) {
		map.put(PROCESS_CPU, "Unknown");
		map.put(PROCESS_VM_RSS, "0");
		map.put(PROCESS_START, "Unknown");
		String cmd = String.format("ps -up %s", getPId());
		IOException lastError = null;
		for (int i = 0; i < 3; ++i) {
			InputStream ins = null;
			Process exec = null;
			try {
				exec = Runtime.getRuntime().exec(cmd);
				ins = exec.getInputStream();

				String output = new String(ByteStreams.toByteArray(ins),
						"UTF-8");
				String[] lines = output.split("\n");
				if (lines.length != 2) {
					LOG.error("[Star-Storm]Unexpected ps output:" + output);
					return;
				}
				String[] items = lines[1].split("\\s+");
				map.put(PROCESS_CPU, items[2] + "%");
				map.put(PROCESS_VM_RSS, items[5]);
				map.put(PROCESS_START, items[8]);
				return;
			} catch (IOException e) {
				lastError = e;
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e1) {}
			} finally {
				if (ins != null) {
					try {
						ins.close();
					} catch (IOException e) {
					}
				}
				if (exec != null) {
					exec.destroy();
				}
			}
		}
		LOG.error(Tools.formatError("getMemoryUsage(" + cmd + ")", lastError));
	}

	public static boolean checkInt(String svalue) {
		for (int i = 0; i < svalue.length(); ++i) {
			char c = svalue.charAt(i);
			if (c == ' ' || c == '-')
				continue;
			if (c >= '0' && c <= '9')
				continue;
			return false;
		}
		return true;

	}

	public static String[] split(String line, String sperator) {
		List<String> list = new ArrayList<String>();
		int pos = line.indexOf(sperator);
		int last = 0;
		while (pos >= 0) {
			list.add(line.substring(last, pos));
			last = pos + 1;
			pos = line.indexOf(sperator, last);
		}
		list.add(line.substring(last));
		String ss[] = new String[list.size()];
		return list.toArray(ss);
	}

	public static List<byte[]> split(byte[] data, byte[] blocksep) {
		List<byte[]> list = new ArrayList<byte[]>();
		int start = 0;
		for (int i = 0; i < data.length; ++i) {
			int j = 0;
			for (j = 0; j < blocksep.length; ++j) {
				if (data[i + j] != blocksep[j])
					break;
			}
			if (j == blocksep.length) {
				if (start < i) {
					byte[] child = new byte[i - start];
					System.arraycopy(data, start, child, 0, child.length);
					list.add(child);
					start = i + j;
				}
			}
		}
		if (start >= 0) {
			byte[] child = new byte[data.length - start];
			System.arraycopy(data, start, child, 0, child.length);
			list.add(child);
		}
		return list;
	}

}
