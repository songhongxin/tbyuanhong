package com.etao.lz.star.spout;

import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.etao.lz.star.StarConfig;
import com.etao.lz.star.StarLogProtos;
import com.etao.lz.star.Tools;
import com.etao.lz.star.ZKTools;
import com.etao.lz.star.monitor.TTMonitor;
import com.etao.lz.star.monitor.WangWangAlert;
import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.InvalidProtocolBufferException;

public class StarSpout extends BaseRichSpout {
	private static final long serialVersionUID = 6839919432321362295L;

	private LogGenerator _generator;

	private Log LOG = LogFactory.getLog(StarSpout.class);

	private BlockingQueue<byte[]> _queue;
	private SpoutOutputCollector _collector;

	private String name;

	private int max_queue_size = 0;

	private transient long count = 0;
	private StarConfig config;
	private int emit_check_period = 5000;

	public StarSpout(LogGenerator generator, String name, StarConfig conf) {
		this._generator = generator;
		this.name = name;
		this.config = conf;
		max_queue_size = conf.getInt("max_queue_size");
	}

	public LogGenerator getLogGenerator() {
		return _generator;
	}

	private final class TTSpoutMonitor extends TTMonitor {
		private static final long serialVersionUID = -7137025062450787763L;

		@Override
		protected void updateStat() {
			try {
				HashMap<String, String> map = new HashMap<String, String>();
				_generator.getStat(map);
				for (Entry<String, String> entry : map.entrySet()) {
					updateChild(entry.getKey(), entry.getValue());
				}
			} catch (Exception e) {
				LOG.error(Tools.formatError("StarBolt.updateStat", e));
			}
		}

		@Override
		protected int getQueueSize() {
			return _queue.size();
		}

		@Override
		protected long getLastTupleTime() {
			return _generator.getLastTupleTime();
		}

		@Override
		protected long getEmitCount() {
			return count;
		}

		@Override
		protected void reset() {
			count = 0;
		}
	}

	private final class CheckEmitTask extends TimerTask
	{
		private long last_count = 0;
		private WangWangAlert alert;
		
		public CheckEmitTask(String alert_user)
		{
			alert = new WangWangAlert(alert_user);
		}
		@Override
		public void run() {
			if (last_count >= count) {
				alert.alert(
						"No Log Emit On " + config.get("topic"),
						String.format(
								"No log has been received since last %d seconds:<br>topo:%s,<br>topic:%s,<br> subsciber_id:%s,<br> host:%s",
								emit_check_period / 1000,
								config.getTopoName(),
								config.get("topic"),
								config.get("subscriber"),
								Tools.getIdAndHost()));
			}
			last_count = count;
		}
	}
	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		_collector = collector;

		_queue = new ArrayBlockingQueue<byte[]>(max_queue_size);
		TTMonitor monitor = new TTSpoutMonitor();
		
		String zookeeperCluster = config.get(StarConfig.StormZookeeperCluster);

		monitor.open(config.getInt("stat_interval"), context.getThisTaskId(),
				config.getTopoName(), name,
				ZKTools.startCuratorClient(zookeeperCluster));
		_generator.open(_queue, name, context.getThisTaskId());

		emit_check_period = config.getInt("emit_check_period");
		
		if (emit_check_period > 0) {
			Timer timer = new Timer();
			timer.schedule(new CheckEmitTask(config.get("alert_user")), emit_check_period, emit_check_period);
		}
	}

	@Override
	public void nextTuple() {
		byte[] msg = null;
		try {
			msg = _queue.take();
		} catch (InterruptedException e) {
			LOG.error(Tools.formatError("StarSpout Error", e));
			return;
		}
		if (msg == null)
			return;
		++count;
		
		//以下做了特殊处理，按shopid 分组，只适合小桥流水项目
		@SuppressWarnings("rawtypes")
		GeneratedMessage.Builder builder = StarConfig.createBuilder("flow");   //add by yuanhong   流量日志
		try {
			builder.mergeFrom(msg);
		} catch (InvalidProtocolBufferException e) {
			LOG.error(Tools.formatError("Log Process Error", e));
			return;
		}
		StarLogProtos.FlowStarLog.Builder log = (StarLogProtos.FlowStarLog.Builder) builder;
        String shopid = log.getShopid();   //----------add  by  yuanhong
		//_collector.emit(new Values(msg, ""), "1");
		_collector.emit(new Values(msg, "",shopid), "1");

	}

	@Override
	public void fail(Object msgId) {
		super.fail(msgId);
		LOG.error("tuple with id " + msgId + "is failed");
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
//		declarer.declare(new Fields("log", "info"));  
		declarer.declare(new Fields("log", "info","shopid"));   //modify by yuanhong
	}

}
