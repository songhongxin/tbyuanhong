package com.etao.lz.star;

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import au.com.bytecode.opencsv.CSVReader;

import com.etao.lz.star.StarLogProtos;
import com.google.protobuf.Descriptors.FieldDescriptor;

public class GenericLoader {

	/**
	 * 日志文件格式描述
	 * 
	 * @author wxz
	 */
	public static interface LogFmtDesc {
		/** 返回日志格式描述字符串 */
		String getLogFmt();

		/** 设置日志路径 */
		void setLogPath(String logPath);

		/** 是否存在下一行数据 */
		boolean hasNext();

		/** 返回日志字段列表 */
		String[] fieldNames();

		/** 返回下一行数据中各个字段值 */
		String[] nextFields();

		/** 善后清理 */
		void cleanup();
	}

	/**
	 * 较为通用的业务测试数据加载接口（日志首行为字段名列表）
	 * 
	 * @param logPath
	 *            测试数据相对或绝对路径
	 * @param fmtDesc
	 *            测试数据文件格式描述类
	 * @return PB 格式的业务日志记录列表
	 * @throws Exception
	 */
	public static List<StarLogProtos.BusinessStarLog> genericLoadBizDataToPB(
			String logPath, LogFmtDesc fmtDesc) throws Exception {
		if (!logPath.startsWith("/")) {
			// 相对路径作为系统资源处理
			logPath = ClassLoader.getSystemResource(logPath).getFile();
		}

		// 建立PB字段名及描述符、字段类型对应表，以便动态更新字段值
		Map<String, FieldDescriptor> pbFieldMap = new HashMap<String, FieldDescriptor>();
		Map<String, Class<?>> pbFieldClazz = new HashMap<String, Class<?>>();
		List<FieldDescriptor> fields = StarLogProtos.BusinessStarLog
				.getDescriptor().getFields();
		for (FieldDescriptor field : fields) {
			pbFieldMap.put(field.getName(), field);
			pbFieldClazz.put(field.getName(), field.getDefaultValue()
					.getClass());
		}

		StarLogProtos.BusinessStarLog.Builder sfb = StarLogProtos.BusinessStarLog
				.newBuilder();
		List<StarLogProtos.BusinessStarLog> res = new ArrayList<StarLogProtos.BusinessStarLog>();

		fmtDesc.setLogPath(logPath);
		String[] fieldNames = fmtDesc.fieldNames();
		// 读取每行日志数据
		while (fmtDesc.hasNext()) {
			sfb.clear();

			String[] l = fmtDesc.nextFields();
			for (int i = 0; i < l.length; i++) {
				String fname = fieldNames[i];
				FieldDescriptor fdesc = pbFieldMap.get(fname);

				// 反射方式获取字段对应的静态 valueOf() 方法
				Class<?> fclazz = pbFieldClazz.get(fname);
				Method statValOf = null;
				try {
					statValOf = fclazz.getMethod("valueOf", String.class);
				} catch (Exception e) {
					// String.valueOf(...) 只接收 Object 类型的参数
					statValOf = fclazz.getMethod("valueOf", Object.class);
				}

				// 调用对应类型的 valueOf() 方法将字段字符串转换为对应类型后初始化 PB 字段
				Object val = statValOf.invoke(null, l[i]);
				sfb.setField(fdesc, val);
			}

			StarLogProtos.BusinessStarLog logEntry = sfb.build();
			res.add(logEntry);
		}
		fmtDesc.cleanup();

		return res;
	}

	/**
	 * 较为通用的流量测试数据加载接口（日志首行为字段名列表）
	 * 
	 * @param logPath
	 *            测试数据相对或绝对路径
	 * @param fmtDesc
	 *            测试数据文件格式描述类
	 * @return PB 格式的流量日志记录列表
	 * @throws Exception
	 */
	public static List<StarLogProtos.FlowStarLog> genericLoadFlowDataToPB(
			String logPath, LogFmtDesc fmtDesc) throws Exception {
		if (!logPath.startsWith("/")) {
			// 相对路径作为系统资源处理
			logPath = ClassLoader.getSystemResource(logPath).getFile();
		}

		// 建立PB字段名及描述符对应表，以便动态更新字段值
		Map<String, FieldDescriptor> pbFieldMap = new HashMap<String, FieldDescriptor>();
		Map<String, Class<?>> pbFieldClazz = new HashMap<String, Class<?>>();
		List<FieldDescriptor> fields = StarLogProtos.FlowStarLog
				.getDescriptor().getFields();
		for (FieldDescriptor field : fields) {
			pbFieldMap.put(field.getName(), field);
			pbFieldClazz.put(field.getName(), field.getDefaultValue()
					.getClass());
		}

		StarLogProtos.FlowStarLog.Builder sfb = StarLogProtos.FlowStarLog
				.newBuilder();
		List<StarLogProtos.FlowStarLog> res = new ArrayList<StarLogProtos.FlowStarLog>();

		fmtDesc.setLogPath(logPath);
		String[] fieldNames = fmtDesc.fieldNames();
		// 读取每行日志数据
		while (fmtDesc.hasNext()) {
			sfb.clear();

			String[] l = fmtDesc.nextFields();
			for (int i = 0; i < l.length; i++) {
				String fname = fieldNames[i];
				FieldDescriptor fdesc = pbFieldMap.get(fname);

				// 反射方式获取字段对应的静态 valueOf() 方法
				Class<?> fclazz = pbFieldClazz.get(fname);
				Method statValOf = null;
				try {
					statValOf = fclazz.getMethod("valueOf", String.class);
				} catch (Exception e) {
					// String.valueOf(...) 只接收 Object 类型的参数
					statValOf = fclazz.getMethod("valueOf", Object.class);
				}

				// 调用对应类型的 valueOf() 方法将字段字符串转换为对应类型后初始化 PB 字段
				Object val = statValOf.invoke(null, l[i]);
				sfb.setField(fdesc, val);
			}

			// 若没有 puid 字段，这里拼一个进去
			if (!sfb.hasPuid()) {
				if (sfb.hasUid() && sfb.getUid().trim().length() > 0) {
					sfb.setPuid(sfb.getUid() + ":");
				} else {
					sfb.setPuid(":" + sfb.getSid());
				}
			}

			StarLogProtos.FlowStarLog logEntry = sfb.build();
			res.add(logEntry);
		}
		fmtDesc.cleanup();

		return res;
	}

	public static class RawLogFmtDesc implements LogFmtDesc {
		Scanner scanner;
		String encoding;
		String delimitRegex;
		String[] fieldNames;

		public RawLogFmtDesc(String delRegex, String enc) {
			delimitRegex = delRegex;
			encoding = enc;
		}

		@Override
		public String getLogFmt() {
			return "raw";
		}

		@Override
		public void setLogPath(String logPath) {
			try {
				scanner = new Scanner(new FileInputStream(logPath), encoding);

				// 日志首行定义为字段名列表
				String headerLine = scanner.nextLine();
				fieldNames = headerLine.split(delimitRegex);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		@Override
		public String[] fieldNames() {
			return fieldNames;
		}

		@Override
		public boolean hasNext() {
			return scanner.hasNext();
		}

		@Override
		public String[] nextFields() {
			String line = scanner.nextLine();
			String[] res = line.split(delimitRegex);
			return res;
		}

		@Override
		public void cleanup() {
			if (scanner != null) {
				scanner.close();
			}
		}

	}

	public static class CsvLogFmtDesc implements LogFmtDesc {

		CSVReader reader;
		String[] fieldNames;
		String[] fields;
		String encoding;
		char separator = ',';
		char quotechar = '"';
		char escape = '\\';
		int skiplines = 0;

		public CsvLogFmtDesc(String enc, int skip) {
			encoding = enc;
			skiplines = skip;
		}

		public CsvLogFmtDesc(String enc, int skip, char sep, char quote,
				char esc) {
			this(enc, skip);
			separator = sep;
			quotechar = quote;
			escape = esc;
		}

		@Override
		public String getLogFmt() {
			return "csv";
		}

		@Override
		public void setLogPath(String logPath) {
			try {
				InputStreamReader isReader = new InputStreamReader(
						new FileInputStream(logPath), encoding);
				reader = new CSVReader(isReader, separator, quotechar, escape,
						skiplines);

				// 首行为日志字段名列表
				fieldNames = reader.readNext();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		@Override
		public String[] fieldNames() {
			return fieldNames;
		}

		@Override
		public boolean hasNext() {
			try {
				fields = reader.readNext();
				if (fields != null) {
					return true;
				}
			} catch (Exception e) {
			}
			return false;
		}

		@Override
		public String[] nextFields() {
			return fields;
		}

		@Override
		public void cleanup() {
			if (reader != null) {
				try {
					reader.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

	}

}
