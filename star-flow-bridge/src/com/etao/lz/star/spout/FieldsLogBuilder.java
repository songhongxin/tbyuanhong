package com.etao.lz.star.spout;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.etao.lz.star.StarConfig;
import com.etao.lz.star.Tools;
import com.google.protobuf.GeneratedMessage;

@SuppressWarnings("rawtypes")
public class FieldsLogBuilder implements TTLogBuilder {
	private Log LOG = LogFactory.getLog(FieldsLogBuilder.class);
	private String sperator[];
	private FieldMappingManager mappingManager;
	private int errorCount = 0;
	private String errorMsg = "";

	private GeneratedMessage.Builder builder;
	private StarConfig config;

	public StarConfig getConfig() {
		return config;
	}

	public int getErrorCount() {
		return errorCount;
	}

	public void config(StarConfig config) {
		this.config = config;
		String seperators = config.get("seperators");
		String[] split = seperators.split(",");
		if (split.length == 0) {
			throw new IllegalStateException("No seperators");
		}
		sperator = new String[split.length];
		for (int i = 0; i < split.length; ++i) {
			sperator[i] = String.valueOf((char) Integer.parseInt(split[i]));
		}

		String logClass = config.getLogClass();

		LOG.info(String.format("seperator=%s, log_class=%s", seperators,
				logClass));
		this.builder = StarConfig.createBuilder(logClass);
		this.mappingManager = new FieldMappingManager(logClass,
				config.getLogMappings());
	}

	@Override
	public List<GeneratedMessage> build(String tag, byte[] msg) {
		List<GeneratedMessage> result = new ArrayList<GeneratedMessage>();
		List<byte[]> messages = Tools.split(msg,
				TTLogReader.TIMETUNNEL_DELIMITER);
		for (byte[] data : messages) {
			if (data.length == 0)
				continue;

			String line;
			try {
				line = new String(data, "utf-8");
			} catch (UnsupportedEncodingException e) {
				throw new IllegalStateException(e);
			}
			builder.clear();
			String[] first_fields = Tools.split(line, sperator[0]);

			try {
				if (!buildLog(tag, first_fields, builder))
					continue;
			} catch (LogFormatException e) {
				errorMsg = e.getMessage();
				++errorCount;
				if ((errorCount % 10000) == 0) {
					LOG.error(Tools.formatError(String.format(
							"Wrong business Log, error count=%d, log=%s",
							errorCount, line), e));
				}
				continue;
			}
			GeneratedMessage log = (GeneratedMessage)builder.build();
			result.add(log);
		}
		return result;
	}

	protected boolean buildLog(String tag, String[] log_fields, GeneratedMessage.Builder builder)
			throws LogFormatException {
		mappingManager.setFields(log_fields, sperator);
		mappingManager.doFieldMapping(tag, builder);
		return true;
	}

	@Override
	public String getLastErrorMsg() {
		return errorMsg;
	}
}
