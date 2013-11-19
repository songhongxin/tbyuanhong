package com.etao.lz.star.spout;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import com.etao.lz.star.EscapeTools;
import com.etao.lz.star.Tools;
import com.etao.lz.star.spout.Aplus.AplusLog;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.GeneratedMessage;

@SuppressWarnings("rawtypes")
public class FieldMappingManager {
	private Map<String, FieldProcessor> fieldProcessorMap;
	private Map<String, FieldDescriptor> aplusFields;
	private Map<String, FieldDescriptor> logFields;
	private LogMapping logMapping;
	private Stack<String> stack;
	private String[] fields;
	private String[][] level2fields;
	private String[][][] level3fields;
	private String[] seperators;
	private AplusLog aplus;

	public FieldMappingManager(String logClass, LogMapping logMapping) {
		this.fieldProcessorMap = new HashMap<String, FieldProcessor>();
		aplusFields = new HashMap<String, FieldDescriptor>();
		logFields = new HashMap<String, FieldDescriptor>();
		this.logMapping = logMapping;
		this.stack = new Stack<String>();
		for (FieldDescriptor field : AplusLog.getDescriptor().getFields()) {
			aplusFields.put(field.getName(), field);
		}
		for (FieldDescriptor field : Tools.getDescriptor(logClass).getFields()) {
			logFields.put(field.getName(), field);
		}
		// multiply first param with second param
		fieldProcessorMap.put("@mul", new FieldProcessor(fieldProcessorMap) {

			@Override
			public Object process(String field, Stack<String> params)
					throws LogFormatException {
				Object o1 = evalParam(field, params);
				Object o2 = evalParam(field, params);
				if (o1 == null || o1.equals("\\N") || o1.equals("")) {
					return null;
				}
				if (o2 == null || o2.equals("\\N") || o2.equals("")) {
					return null;
				}
				try {
					return Long.parseLong(o1.toString())
							* Long.parseLong(o2.toString());
				} catch (NumberFormatException e) {
					throw new LogFormatException("expect integer, but was "
							+ o1 + "," + o2);
				}
			}

		});
		// transform string to int,require a string parameter which can be
		// converted to int
		fieldProcessorMap.put("@int", new FieldProcessor(fieldProcessorMap) {

			@Override
			public Object process(String field, Stack<String> params)
					throws LogFormatException {
				Object o = evalParam(field, params);
				if (o == null || o.equals("\\N") || o.equals("")) {
					return null;
				}
				try {
					return Integer.parseInt(o.toString());
				} catch (NumberFormatException e) {
					throw new LogFormatException("expect integer, but was " + o);
				}
			}

		});
		// transform string to bigint,require a string parameter which can be
		// converted to int
		fieldProcessorMap.put("@bigint", new FieldProcessor(fieldProcessorMap) {

			@Override
			public Object process(String field, Stack<String> params)
					throws LogFormatException {
				Object o = evalParam(field, params);
				if (o == null || o.equals("\\N") || o.equals("")) {
					return null;
				}
				try {
					return Long.parseLong(o.toString());
				} catch (NumberFormatException e) {
					throw new LogFormatException("expect long, but was " + o);
				}
			}

		});
		// fetch log field,require three string parameter which can be converted
		// to int
		fieldProcessorMap.put("@field3", new FieldProcessor(fieldProcessorMap) {

			@Override
			public Object process(String field, Stack<String> params)
					throws LogFormatException {
				if (fields == null) {
					throw new IllegalStateException("Not a fields log");
				}
				Object o1 = evalParam(field, params);
				Object o2 = evalParam(field, params);
				Object o3 = evalParam(field, params);
				if (o1 == null || o2 == null || o3 == null) {
					return null;
				}
				try {
					int i = Integer.parseInt(o1.toString());
					int j = Integer.parseInt(o2.toString());
					int k = Integer.parseInt(o3.toString());
					return getLevel3Fields(i, j, k);
				} catch (NumberFormatException e) {
					throw new LogFormatException("expect integer, but was "
							+ o1 + "," + o2);
				}
			}
		});
		// fetch log field,require two string parameter which can be converted
		// to int
		fieldProcessorMap.put("@field2", new FieldProcessor(fieldProcessorMap) {

			@Override
			public Object process(String field, Stack<String> params)
					throws LogFormatException {
				if (fields == null) {
					throw new IllegalStateException("Not a fields log");
				}
				Object o1 = evalParam(field, params);
				Object o2 = evalParam(field, params);
				if (o1 == null || o2 == null) {
					return null;
				}
				try {
					int i = Integer.parseInt(o1.toString());
					int j = Integer.parseInt(o2.toString());
					return getLevel2Fields(i, j);
				} catch (NumberFormatException e) {
					throw new LogFormatException("expect integer, but was "
							+ o1 + "," + o2);
				}
			}
		});
		// fetch log field,require one string parameter which can be converted
		// to int
		fieldProcessorMap.put("@field", new FieldProcessor(fieldProcessorMap) {

			@Override
			public Object process(String field, Stack<String> params)
					throws LogFormatException {
				if (fields == null) {
					throw new IllegalStateException("Not a fields log");
				}
				Object o1 = evalParam(field, params);
				if (o1 == null) {
					return null;
				}
				try {
					int i = Integer.parseInt(o1.toString());
					return getLevel1Fields(i);
				} catch (NumberFormatException e) {
					throw new LogFormatException("expect integer, but was "
							+ o1);
				}
			}
		});

		// fetch log member,require a string parameter which is a bean property
		fieldProcessorMap.put("@aplus_bytes", new FieldProcessor(
				fieldProcessorMap) {

			@Override
			public Object process(String field, Stack<String> params)
					throws LogFormatException {
				String property = evalParam(field, params).toString();
				if (aplus == null) {
					throw new IllegalStateException("Not a fields log");
				}
				try {
					FieldDescriptor fd = aplusFields.get(property);

					if (fd == null) {
						throw new LogFormatException("Failed to fetch member:"
								+ property);
					}
					ByteString value = (ByteString) aplus.getField(fd);
					return value.toStringUtf8();
				} catch (Exception e) {
					throw new LogFormatException("Failed to fetch member:"
							+ property);
				}
			}
		});

		// convert parameter double string to double and multiply it with 100,
		// then convert it to bigint
		fieldProcessorMap.put("@price_cent", new FieldProcessor(
				fieldProcessorMap) {

			@Override
			public Object process(String field, Stack<String> params)
					throws LogFormatException {
				String o = evalParam(field, params).toString();
				if (o.equals("\\N"))
					return 0L;
				try {
					return (long) Double.parseDouble(o) * 100;
				} catch (NumberFormatException e) {
					throw new LogFormatException(e.getMessage());
				}
			}
		});

		fieldProcessorMap.put("@unescape", new FieldProcessor(
				fieldProcessorMap) {

			@Override
			public Object process(String field, Stack<String> params)
					throws LogFormatException {
				String o = evalParam(field, params).toString();
				try {
					return EscapeTools.unescape(o);
				} catch (Exception e) {
					return o;
				}
			}
		});
	}

	public void doFieldMapping(String mappingName,
			GeneratedMessage.Builder builder) throws LogFormatException {
		List<LogMappingItem> mappings = logMapping.getMapping(mappingName);
		if (mappings == null) {
			throw new IllegalStateException("Unknown Mapping " + mappingName);
		}

		for (int i = 0; i < mappings.size(); ++i) {
			LogMappingItem mapping = mappings.get(i);
			FieldDescriptor d = logFields.get(mapping.getName());
			if (d == null) {
				throw new IllegalStateException("Unexpected field "
						+ mapping.getName());
			}
			Object result = evalFieldValue(mapping.getName(), mapping);
			if (result == null)
				continue;
			builder.setField(d, result);
		}
	}

	private Object evalFieldValue(String field, LogMappingItem mapping)
			throws LogFormatException {
		mapping.fillCommandStack(stack);
		if (stack.empty()) {
			return "";
		}
		String cmd = stack.pop();
		if (!cmd.startsWith("@")) {
			return cmd;
		}
		FieldProcessor processor = fieldProcessorMap.get(cmd);
		if (processor == null) {
			throw new IllegalStateException("Unrecognized command " + cmd);
		}

		return processor.process(field, stack);
	}

	public void setFields(String[] fields, String[] seperators) {
		this.fields = fields;
		this.level2fields = new String[fields.length][];
		this.level3fields = new String[fields.length][][];
		this.seperators = seperators;
	}

	public void setAplus(AplusLog aplus) {
		this.aplus = aplus;
	}

	private String getLevel1Fields(int i) throws LogFormatException {
		if (seperators.length == 0) {
			throw new IllegalStateException("Level 1 seperator is not defined!");
		}
		if (i == 0 || i > fields.length) {
			throw new LogFormatException("Field level 1 index out of bound:"
					+ i);
		}
		return fields[i - 1];
	}

	private String getLevel2Fields(int i, int j) throws LogFormatException {
		if (seperators.length < 2) {
			throw new IllegalStateException("Level 2 seperator is not defined!");
		}
		String ff = getLevel1Fields(i);
		if (level2fields[i - 1] == null) {
			level2fields[i - 1] = Tools.split(ff, seperators[1]);
			level3fields[i - 1] = new String[level2fields[i - 1].length][];
		}
		if (j == 0 || j > level2fields[i - 1].length) {
			throw new LogFormatException("Field level 2 index out of bound:"
					+ j + ", log:" + ff);
		}
		return level2fields[i - 1][j - 1];
	}

	private String getLevel3Fields(int i, int j, int k)
			throws LogFormatException {
		if (seperators.length < 3) {
			throw new IllegalStateException("Level 3 seperator is not defined!");
		}
		if (j == 0) {
			String ff = getLevel1Fields(i);
			if (ff.trim().length() == 0)
				return "";
			if (level2fields[i - 1] == null) {
				level2fields[i - 1] = Tools.split(ff, seperators[1]);
				level3fields[i - 1] = new String[level2fields[i - 1].length][];
			}
			StringBuilder sb = new StringBuilder();
			for (j = 1; j <= level2fields[i - 1].length; ++j) {
				ff = level2fields[i - 1][j - 1];
				if (level3fields[i - 1][j - 1] == null) {
					level3fields[i - 1][j - 1] = Tools.split(ff, seperators[2]);
				}
				if (k == 0 || k > level3fields[i - 1][j - 1].length) {
					throw new LogFormatException(
							"Field level 3 index out of bound:" + k);
				}
				sb.append(level3fields[i - 1][j - 1][k - 1]);
				sb.append('\001');
			}
			sb.delete(sb.length() - 1, sb.length());
			return sb.toString();
		}
		String ff = getLevel2Fields(i, j);
		if (level3fields[i - 1][j - 1] == null) {
			level3fields[i - 1][j - 1] = Tools.split(ff, seperators[2]);
		}
		if (k == 0 || k > level3fields[i - 1][j - 1].length) {
			throw new LogFormatException("Field level 3 index out of bound:"
					+ k);
		}
		return level3fields[i - 1][j - 1][k - 1];
	}
}
