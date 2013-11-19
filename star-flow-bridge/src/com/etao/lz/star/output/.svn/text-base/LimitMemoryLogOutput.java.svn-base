package com.etao.lz.star.output;

import java.util.HashMap;
import java.util.Map;
import java.util.TimerTask;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.etao.lz.star.StarConfig;
import com.etao.lz.star.Tools;
import com.etao.lz.star.monitor.WangWangAlert;
import com.google.protobuf.GeneratedMessage;

public class LimitMemoryLogOutput implements LogOutput {

	private static final long serialVersionUID = 358390532077833100L;
	private LogOutput target;
	private StarConfig config;

	private int memLimit;
	private int ignoreCount;
	private volatile int ignoreLeft;

	private transient WangWangAlert alert;
	protected Log LOG = LogFactory.getLog(getClass());
	
	private transient java.util.Timer timer;

	@SuppressWarnings("rawtypes")
	@Override
	public void config(StarConfig config) {
		this.config = config;
		try {
			Class cls = Class.forName(config.get("limitmemory_target"));
			target = (LogOutput) cls.newInstance();
		} catch (Exception e) {
			throw new IllegalStateException(e);
		}
		target.config(config);
	}

	@Override
	public void open(int id) {

		ignoreCount = config.getInt("ignore_count");
		memLimit = config.getInt("memory_limit");
		int checkInterval = config.getInt("mem_check_interval");
		timer = new java.util.Timer();
		timer.schedule(new TimerTask() {

			@Override
			public void run() {
				HashMap<String, String> processInfo = new HashMap<String, String>();
				try {
					Tools.getProcessInfo(processInfo);
					long memoryUsage = Long.parseLong(processInfo
							.get(Tools.PROCESS_VM_RSS));
					if (memoryUsage > memLimit) {
						ignoreLeft = ignoreCount;
						alert.alert(
								"WriteToHBaseProcessor Memory Alert",
								String.format(
										"%s uses memory %d kb, exceed limit %d kb, following %d log will be thrown away!",
										Tools.getIdAndHost(), memoryUsage,
										memLimit, ignoreCount));
					}
				} catch (Exception e) {
					LOG.error(Tools.formatError(String.format("LimitMemoryLogOutput Timer:"), e));
				}
			}

		}, checkInterval, checkInterval);

		ignoreLeft = 0;
		alert = new WangWangAlert(config.get("alert_user"));
		target.open(id);
	}

	@Override
	public void output(byte[] data, GeneratedMessage log) {
		if (ignoreLeft > 0) {
			--ignoreLeft;
			return;
		}
		target.output(data, log);
	}

	@Override
	public void getStat(Map<String, String> map) {
		target.getStat(map);
	}

}
