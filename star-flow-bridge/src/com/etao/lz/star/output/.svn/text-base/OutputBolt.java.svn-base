package com.etao.lz.star.output;

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
import backtype.storm.tuple.Tuple;

import com.etao.lz.star.StarConfig;
import com.etao.lz.star.Tools;
import com.etao.lz.star.ZKTools;
import com.etao.lz.star.monitor.TTMonitor;
import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.InvalidProtocolBufferException;

@SuppressWarnings("rawtypes")
public class OutputBolt extends BaseRichBolt {
	public static final String ZK_OUTPUT_DIR = "output_bolt";
	private static final long serialVersionUID = -2778554064735646174L;
	private Log LOG = LogFactory.getLog(OutputBolt.class);
	private List<LogOutput> outputList;
	private String log_type;
	private transient long count;
	private transient long lastTs;
	private transient OutputCollector collector;
	private StarConfig config;
	public OutputBolt(StarConfig config) {
		this.config = config;
		this.outputList = new ArrayList<LogOutput>();
		this.log_type = config.getLogType();
	}

	public void addOutputProcessor(LogOutput output) {
		outputList.add(output);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		this.count = 0;
		this.lastTs = 0;
		TTMonitor monitor = new TTMonitor() {

			private static final long serialVersionUID = -7137025062450787763L;

			@Override
			protected void updateStat() {
				for (LogOutput out : outputList) {
					try {
						HashMap<String,String> map = new HashMap<String,String>();
						out.getStat(map);
						for(Entry<String,String> entry: map.entrySet())
						{
							updateChild(out.getClass().getSimpleName() + "_" + entry.getKey(), entry.getValue());
						}
						
					} catch (Exception e) {
						LOG.error(Tools.formatError("OutputBolt.updateStat", e));
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

		monitor.open(config.getInt("stat_interval"),
				context.getThisTaskId(), 
				config.getTopoName(),
				ZK_OUTPUT_DIR,
				ZKTools.startCuratorClient(zookeeperCluster));
		for (LogOutput out : outputList) {
			out.open(context.getThisTaskId());
		}
	}

	private void process(Tuple input) {
		byte[] data = (byte[]) input.getValueByField("log");
		++count;
		
		GeneratedMessage.Builder builder = StarConfig.createBuilder(log_type);
		try {
			builder.mergeFrom(data);
		} catch (InvalidProtocolBufferException e) {
			LOG.error(Tools.formatError("Log Output Error:%s", e));
			return;
		}

		GeneratedMessage message = (GeneratedMessage) builder.build();
		lastTs = config.getLogTime(message);
		try {
			for (LogOutput out : outputList) {
				out.output(data, message);
			}
		} catch (Throwable e) {
			LOG.error(Tools.formatError("Log Output Error", e));
			return;
		}
	}

	@Override
	public void execute(Tuple input) {
		process(input);
		collector.ack(input);
	}
}
