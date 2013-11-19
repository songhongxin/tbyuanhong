package com.etao.lz.star.bolt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.etao.lz.star.StarConfig;
import com.etao.lz.star.StarLogProtos;
import com.etao.lz.star.Tools;
import com.etao.lz.star.ZKTools;
import com.etao.lz.star.monitor.TTMonitor;
import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.InvalidProtocolBufferException;

@SuppressWarnings({ "rawtypes" })
public class StarBolt extends BaseRichBolt {
	public static final String ZK_OUTPUT_DIR = "filling_bolt";

	private static final long serialVersionUID = 2025379444267408417L;

	private Log LOG = LogFactory.getLog(StarBolt.class);

	private List<LogProcessor> _processors;

	private String _logType;
	private transient OutputCollector _collector;
	private StarConfig config;
	private transient long count;
	private transient long lastTs;

	public StarBolt(String logType, StarConfig config) {
		this._logType = logType;
		this.config = config;
		LOG.info(_logType);
		this._processors = new ArrayList<LogProcessor>();
	}

	public void addProcessor(LogProcessor p) {
		_processors.add(p);
	}

	public int getProcessorCount() {
		return _processors.size();
	}

	public LogProcessor getProcessorInfo(int i) {
		return _processors.get(i);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("log", "info"));
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this._collector = collector;
		this.count = 0;
		this.lastTs = 0;
		TTMonitor monitor = new TTMonitor() {

			private static final long serialVersionUID = -7137025062450787763L;

			@Override
			protected void updateStat() {
				for (LogProcessor info : _processors) {
					try {
						HashMap<String, String> map = new HashMap<String, String>();
						info.getStat(map);
						for (Entry<String, String> entry : map.entrySet()) {
							updateChild(info.getClass().getSimpleName() + "_"
									+ entry.getKey(), entry.getValue());
						}
					} catch (Exception e) {
						LOG.error(Tools.formatError("StarBolt.updateStat", e));
					}
				}
			}

			@Override
			protected int getQueueSize() {
				return -1;
			}

			@Override
			protected long getLastTupleTime() {
				return lastTs;
			}

			@Override
			protected long getEmitCount() {
				return count;
			}

			@Override
			protected void reset() {
				count = 0;
			}
		};

		String zookeeperCluster = config.get(StarConfig.StormZookeeperCluster);

		monitor.open(config.getInt("stat_interval"), context.getThisTaskId(),
				config.getTopoName(), ZK_OUTPUT_DIR,
				ZKTools.startCuratorClient(zookeeperCluster));
		for (LogProcessor p : _processors) {
			p.open(context.getThisTaskId());
		}

	}

	protected void process(GeneratedMessage.Builder builder) {
		for (LogProcessor p : _processors) {
			if (!p.accept(builder))
				continue;
			p.process(builder);

		}
	}

	private void process(Tuple input) {
		byte[] data = (byte[]) input.getValueByField("log");

		GeneratedMessage.Builder builder = StarConfig.createBuilder(_logType);

		try {
			builder.mergeFrom(data);
		} catch (InvalidProtocolBufferException e) {
			LOG.error(Tools.formatError("Log Process Error", e));
			return;
		}
		lastTs = config.getLogTime(builder);
		++count;
		try {
			process(builder);
		} catch (Throwable e) {
			LOG.error(Tools.formatError("Log Process Error", e));
			return;
		}

		StarLogProtos.FlowStarLog.Builder log = (StarLogProtos.FlowStarLog.Builder) builder;
		if (log.getUnitId() > 0) {
			byte[] output = builder.build().toByteArray();
			_collector.emit(input, new Values(output, ""));
		}

	}

	@Override
	public void execute(Tuple input) {
		process(input);
		_collector.ack(input);
	}

}
