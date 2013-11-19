package com.etao.lz.star.spout;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.etao.lz.star.StarConfig;
import com.etao.lz.star.StarLogProtos.FlowStarLog;
import com.etao.lz.star.Tools;
import com.etao.lz.star.spout.Aplus.AplusLog;
import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.taobao.timetunnel.client.parser.MessageParser;

public class APlusLogBuilder implements TTLogBuilder {

	private Log LOG = LogFactory.getLog(APlusLogBuilder.class);
	private int errorCount = 0;
	private String errorMsg = "";
	private FlowStarLog.Builder builder;
	private FieldMappingManager mappingManager;
	public int getErrorCount() {
		return errorCount;
	}

	public void config(StarConfig config) {
		this.builder = FlowStarLog.newBuilder();
		String logClass = config.getLogClass();
		mappingManager = new FieldMappingManager(logClass, config.getLogMappings());
		LOG.info(String.format("APlusLogBuilder log_class=%s", logClass));
	}
	
	public List<GeneratedMessage> build(String tag, byte[] data) {
		List<GeneratedMessage> result = new ArrayList<GeneratedMessage>();
		
		List<byte[]> split = MessageParser.parseProtoBufsFromBytes(data);
		for (byte[] logData : split) {
			try {
				AplusLog log = Aplus.AplusLog.parseFrom(logData);
				mappingManager.setAplus(log);
				builder.clear();
				mappingManager.doFieldMapping(tag, builder);

				long ts = log.getTime();
				builder.setTs(ts * 1000);
				builder.setIp(Tools.getIp(log.getIp()));
				if (!log.getAtShoptype().isEmpty()) {
					String ss = log.getAtShoptype().toStringUtf8();
					int pos = ss.indexOf("_");
					if (pos >= 0) {
						builder.setShopid(ss.substring(pos + 1));
					}
				} else if (!log.getAtAutype().isEmpty()) {
					String ss = log.getAtAutype().toStringUtf8();
					int pos = ss.indexOf("_");
					if (pos >= 0) {
						builder.setShopid(ss.substring(pos + 1));
					}
				}
				if(builder.getShopid().isEmpty())
					continue;                           //add by yuanhong.shx 小桥流水项目
				result.add(builder.build());
			} catch (InvalidProtocolBufferException e) {
				++errorCount;
				errorMsg = e.getMessage();
				if ((errorCount % 10000) == 0) {
					LOG.error(Tools.formatError(String.format(
							"Wrong aplus Log, error count=%d", errorCount), e));
				}
			} catch (LogFormatException e) {
				++errorCount;
				errorMsg = e.getMessage();
				if ((errorCount % 10000) == 0) {
					LOG.error(Tools.formatError(String.format(
							"Wrong aplus Log, error count=%d", errorCount), e));
				}
			}

		}
		return result;

	}

	@Override
	public String getLastErrorMsg() {
		return errorMsg;
	}

}
