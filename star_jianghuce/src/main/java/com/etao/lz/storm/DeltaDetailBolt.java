package com.etao.lz.storm;

import gnu.trove.iterator.TIntObjectIterator;
import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTableInterfaceFactory;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import com.etao.lz.hbase.RowUtils;
import com.etao.lz.star.StarLogProtos;
import com.etao.lz.storm.detailcalc.JhcDeltaDetailCalc;
import com.etao.lz.storm.detailcalc.UnitItem;
import com.etao.lz.storm.tools.Constants;
import com.etao.lz.storm.utils.AdaptiveCounting;
import com.etao.lz.storm.utils.HBaseUtil;
import com.etao.lz.storm.utils.IndicatorUtil;

/**
 * 江湖策指标增量计算 bolt
 */
@Slf4j
public class DeltaDetailBolt extends BaseRichBolt {

	static final long serialVersionUID = -5660298004139920055L;

	private static final byte[] FAMILY = Bytes.toBytes("d");
	private static final byte[] COL_PV = Bytes.toBytes("pv"); // pv 浏览量 long/8
	private static final byte[] COL_AMT = Bytes.toBytes("amt"); // amt 支付宝成交金额
																// long/8
	private static final byte[] COL_AN = Bytes.toBytes("an"); // an 支付宝成交件数
																// long/8
	private static final byte[] COL_TN = Bytes.toBytes("tn"); // tn 拍下笔数 long/8

	private static final byte[] COL_ATN = Bytes.toBytes("atn"); // atn 成交笔数
																// long/8
	private static final byte[] COL_AUV = Bytes.toBytes("auv"); // auv 成交uv
																// long/8
	private static final byte[] COL_IUV = Bytes.toBytes("iuv"); // iuv 流量uv
																// long/8
	private static final byte[] COL_AR = Bytes.toBytes("ar"); // ar
																// 成交转化率（int(auv/iuv*10000)）
																// long/8
	private static final byte[] COL_AZ = Bytes.toBytes("az"); // az
																// 当日支付率（int(atn/tn*10000)）
																// long/8
	private static final byte[] COL_AU = Bytes.toBytes("au"); // au
																// 访客价值（int(amt/iuv*10000)）
																// long/8
	private static final byte[] COL_AB = Bytes.toBytes("ab"); // ab 成交uv位图信息
																// byte[]<8192
	private static final byte[] COL_IB = Bytes.toBytes("ib"); // ib 流量uv位图信息
																// byte[]<8192

	static final int FLUSH_INTERVAL = 60000; // HBase 刷新周期
	static final int THREAD_POOL_WORKERS = 5; // HBase 更新线程池大小
	static final int JOB_BATCH_SIZE = 1000; // 任务大小
	static final int MAX_QUEUED_JOBS = 5; // 最大队列累积任务

	/** Storm 任务上下文 */
	int taskId;
	String componentId;
	transient OutputCollector collector;
	transient TTSpoutMonitor _monitor;

	transient JhcDeltaDetailCalc jhcDeltaCalc;

	/** HBase 相关 */
	transient HTablePool hbasePool;
	transient ExecutorService executorService;

	public DeltaDetailBolt() {
	}

	@Override
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf,
			TopologyContext context, OutputCollector collect) {
		taskId = context.getThisTaskId();
		componentId = context.getComponentId(taskId);
		collector = collect;

		String name = Constants.DETAIL_BOLT_ID;
		_monitor = new TTSpoutMonitor(name, stormConf, context);
		_monitor.reset();
		_monitor.open();

		jhcDeltaCalc = new JhcDeltaDetailCalc();

		initHBaseConf();
		startFlushDaemons();
	}

	void initHBaseConf() {
		try {
			Configuration conf = new Configuration();
			conf.addResource("hbase-site-new.xml");
			Configuration hbaseConf = HBaseConfiguration.create(conf);
			hbasePool = new HTablePool(hbaseConf, THREAD_POOL_WORKERS * 2,
					new HTableInterfaceFactory() {
						@Override
						public HTableInterface createHTableInterface(
								Configuration config, byte[] tableName) {
							// 覆盖默认的 HTablePool 构建方法，以便设置 autoflush 及
							// writebuffer
							try {
								HTable htab = new HTable(config, tableName);
								htab.setAutoFlush(false);
								htab.setWriteBufferSize(Constants.HBASE_WBUF_SIZE);
								return htab;
							} catch (IOException e) {
								throw new RuntimeException(e);
							}
						}

						@Override
						public void releaseHTableInterface(HTableInterface table)
								throws IOException {
							table.close();
						}
					});

			BlockingQueue<Runnable> blockingQueue = new ArrayBlockingQueue<Runnable>(
					MAX_QUEUED_JOBS);
			RejectedExecutionHandler rejectedExecutionHandler = new ThreadPoolExecutor.CallerRunsPolicy();
			executorService = new ThreadPoolExecutor(THREAD_POOL_WORKERS,
					THREAD_POOL_WORKERS, 0L, TimeUnit.MILLISECONDS,
					blockingQueue, rejectedExecutionHandler);
		} catch (Exception e) {
			log.error("Failed to initialize HBase", e);
			throw new RuntimeException(e);
		}
	}

	@Override
	public void execute(Tuple input) {
		String streamID = input.getSourceStreamId();
		byte[] logBytes = input.getBinaryByField("log");

		if (logBytes != null && logBytes.length > 1) {
			if (Constants.TRAFFIC_STREAMID.equals(streamID)) {
				// 流量日志处理
				try {
					StarLogProtos.FlowStarLog logEntry = StarLogProtos.FlowStarLog
							.parseFrom(logBytes);
					synchronized (jhcDeltaCalc) {
						jhcDeltaCalc.updateTrafficIndicators(logEntry);
					}
				} catch (Exception e) {
					log.error("Failed to process traffic log", e);
				}
			} else if (Constants.BIZ_STREAMID.equals(streamID)) {
				// 成交日志处理
				try {
					StarLogProtos.BusinessStarLog logEntry = StarLogProtos.BusinessStarLog
							.parseFrom(logBytes);
					synchronized (jhcDeltaCalc) {
						jhcDeltaCalc.updateBusinessIndicators(logEntry);
					}
				} catch (Exception e) {
					log.error("Failed to process biz log", e);
				}
			}

			long ts = input.getLongByField("ts");
			_monitor.sign(ts);
		}
		collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	void blockCommitToThreadPool(Runnable job) {
		// 提交任务到线程池，线程池满时反复重试
		int retryTimes = 0;
		while (true) {
			try {
				executorService.execute(job);
				break;
			} catch (RejectedExecutionException e) {
				log.warn(String.format(
						"Failed to commit job, retried %d times", retryTimes),
						e);
				sleep(500);
				retryTimes++;
				continue;
			}
		}
	}

	void emitHourlyHBaseJobs() {
		TIntObjectHashMap<TLongObjectHashMap<TLongObjectHashMap<UnitItem>>> hourlyDeltas = null;
		List<Get> gets = new ArrayList<Get>();
		List<UnitItem> updates = new ArrayList<UnitItem>();
		List<MergeWriteJob> jobs = new ArrayList<DeltaDetailBolt.MergeWriteJob>();

		// 为了保证原子性，需要对查询更新操作加锁
		synchronized (jhcDeltaCalc) {
			jhcDeltaCalc.dumpInfo();
			hourlyDeltas = jhcDeltaCalc.flushHourlyItems();

			for (TIntObjectIterator<TLongObjectHashMap<TLongObjectHashMap<UnitItem>>> it = hourlyDeltas
					.iterator(); it.hasNext();) {
				it.advance();
				for (TLongObjectIterator<TLongObjectHashMap<UnitItem>> sidIt = it
						.value().iterator(); sidIt.hasNext();) {
					sidIt.advance();
					for (TLongObjectIterator<UnitItem> aidIt = sidIt.value()
							.iterator(); aidIt.hasNext();) {
						aidIt.advance();
						int hourlyTs = it.key();
						int dateYmd = IndicatorUtil.epochToYmd(hourlyTs);
						byte tid = IndicatorUtil.bcdToTid(hourlyTs);
						long sellerId = sidIt.key();
						long auctionId = aidIt.key();
						UnitItem delta = aidIt.value();
						byte[] rowKey = RowUtils.RptDpt.rowKeyHour(dateYmd,
								tid, sellerId, auctionId);

						Get get = new Get(rowKey);
						get.addFamily(FAMILY);
						gets.add(get);

						updates.add(delta);

						if (gets.size() >= JOB_BATCH_SIZE) {
							// 任务分片交给后台线程池并发更新 HBase
							MergeWriteJob job = new MergeWriteJob(
									MergeWriteJob.TYPE_HOURLY, hbasePool, gets,
									updates);
							jobs.add(job);
							// 重置任务列表
							gets = new ArrayList<Get>();
							updates = new ArrayList<UnitItem>();
						}
					}
				}
			}

			if (gets.size() >= 0) {
				// 任务分片交给后台线程池并发更新 HBase
				MergeWriteJob job = new MergeWriteJob(
						MergeWriteJob.TYPE_HOURLY, hbasePool, gets, updates);
				jobs.add(job);
			}
		}

		for (MergeWriteJob job : jobs) {
			blockCommitToThreadPool(job);
		}
	}

	void emitDailyHBaseJobs() {
		TIntObjectHashMap<TLongObjectHashMap<TLongObjectHashMap<UnitItem>>> dailyDeltas = null;
		List<Get> gets = new ArrayList<Get>();
		List<UnitItem> updates = new ArrayList<UnitItem>();
		List<MergeWriteJob> jobs = new ArrayList<DeltaDetailBolt.MergeWriteJob>();

		synchronized (jhcDeltaCalc) {
			dailyDeltas = jhcDeltaCalc.flushDailyItems();

			for (TIntObjectIterator<TLongObjectHashMap<TLongObjectHashMap<UnitItem>>> it = dailyDeltas
					.iterator(); it.hasNext();) {
				it.advance();
				for (TLongObjectIterator<TLongObjectHashMap<UnitItem>> sidIt = it
						.value().iterator(); sidIt.hasNext();) {
					sidIt.advance();
					for (TLongObjectIterator<UnitItem> aidIt = sidIt.value()
							.iterator(); aidIt.hasNext();) {
						aidIt.advance();
						int dailyTs = it.key();
						int dateYmd = IndicatorUtil.epochToYmd(dailyTs);
						long sellerId = sidIt.key();
						long auctionId = aidIt.key();
						UnitItem delta = aidIt.value();
						byte[] rowKey = RowUtils.RptDpt.rowKeyDay(dateYmd,
								sellerId, auctionId);

						Get get = new Get(rowKey);
						get.addFamily(FAMILY);
						gets.add(get);

						updates.add(delta);

						if (gets.size() >= JOB_BATCH_SIZE) {
							// 任务分片交给后台线程池并发更新 HBase
							MergeWriteJob job = new MergeWriteJob(
									MergeWriteJob.TYPE_DAILY, hbasePool, gets,
									updates);
							jobs.add(job);
							// 重置任务列表
							gets = new ArrayList<Get>();
							updates = new ArrayList<UnitItem>();
						}
					}
				}
			}

			if (gets.size() >= 0) {
				// 任务分片交给后台线程池并发更新 HBase
				MergeWriteJob job = new MergeWriteJob(
						MergeWriteJob.TYPE_HOURLY, hbasePool, gets, updates);
				jobs.add(job);
			}
		}

		for (MergeWriteJob job : jobs) {
			blockCommitToThreadPool(job);
		}
	}

	void startFlushDaemons() {
		// 启动刷新分时段累计指标的异步线程
		Thread hourlyFlushThread = new Thread(new Runnable() {
			@Override
			public void run() {
				while (!Thread.interrupted()) {
					try {
						sleep(FLUSH_INTERVAL);

						long start = System.currentTimeMillis();
						emitHourlyHBaseJobs();
						long end = System.currentTimeMillis();
						log.info(String.format(
								"Hourly indicators merged in %f s",
								(end - start) / 1000.0));
					} catch (Exception e) {
						log.error(
								"Failed to sleep and merge write hourly inds",
								e);
					}
				}
			}
		}, "hourly-flush-thread");
		hourlyFlushThread.setDaemon(true);
		hourlyFlushThread.start();

		// 启动刷新当日累计指标的异步线程
		Thread dailyFlushThread = new Thread(new Runnable() {
			@Override
			public void run() {
				while (!Thread.interrupted()) {
					try {
						sleep(FLUSH_INTERVAL);

						long start = System.currentTimeMillis();
						emitDailyHBaseJobs();
						long end = System.currentTimeMillis();
						log.info(String.format(
								"Daily indicators merged in %f s",
								(end - start) / 1000.0));
					} catch (Exception e) {
						log.error("Failed to sleep and merge write daily inds",
								e);
					}
				}
			}
		}, "daily-flush-thread");
		dailyFlushThread.setDaemon(true);
		dailyFlushThread.start();
	}

	void sleep(long ms) {
		try {
			Thread.sleep(ms);
		} catch (Exception e) {
		}
	}

	private final class TTSpoutMonitor extends AppMonitor {

		private static final long serialVersionUID = 2694180049179434218L;

		@SuppressWarnings("rawtypes")
		public TTSpoutMonitor(String name, Map conf, TopologyContext context) {
			super(name, conf, context);
		}

		@Override
		protected int getQueueSize() {
			return 0;
		}
	}

	static class MergeWriteJob implements Runnable {
		static final int TYPE_HOURLY = 0;
		static final int TYPE_DAILY = 1;

		int type;
		HTablePool poolRef;
		List<Get> jobGets;
		List<UnitItem> jobUpdates;

		public MergeWriteJob(int jobType, HTablePool hpRef, List<Get> gets,
				List<UnitItem> updates) {
			type = jobType;
			poolRef = hpRef;
			jobGets = gets;
			jobUpdates = updates;
		}

		@Override
		public void run() {
			if (type == TYPE_HOURLY) {
				mergeWriteHourly(jobGets, jobUpdates);
			} else {
				mergeWriteDaily(jobGets, jobUpdates);
			}
		}

		void mergeWriteHourly(List<Get> gets, List<UnitItem> updates) {
			List<Put> puts = new ArrayList<Put>();
			AdaptiveCounting tmp = null;
			HTableInterface tabShopItemHourly = null;
			try {
				tabShopItemHourly = poolRef.getTable(Constants.DP_H_TABLE);

				Result[] results = tabShopItemHourly.get(gets);
				for (int i = 0; i < results.length; i++) {
					Result res = results[i];
					UnitItem update = updates.get(i);
					byte[] row = gets.get(i).getRow();

					byte[] pvVal = HBaseUtil.addHBaseCol(log, res, update.pv,
							FAMILY, COL_PV);
					byte[] amtVal = HBaseUtil.addHBaseCol(log, res,
							update.alipayAmt, FAMILY, COL_AMT);
					long amt = Bytes.toLong(amtVal);
					byte[] anVal = HBaseUtil.addHBaseCol(log, res,
							update.alipayNum, FAMILY, COL_AN);
					byte[] tnVal = HBaseUtil.addHBaseCol(log, res,
							update.tradeNum, FAMILY, COL_TN);
					long tn = Bytes.toLong(tnVal);
					byte[] atnVal = HBaseUtil.addHBaseCol(log, res,
							update.alipayTradeNum, FAMILY, COL_ATN);
					long atn = Bytes.toLong(atnVal);
					tmp = HBaseUtil.uniqAddHBaseCol(log, res, update.ab,
							FAMILY, COL_AB);
					byte[] abVal = tmp.getBytes();
					long auv = tmp.cardinality();
					byte[] auvVal = Bytes.toBytes(auv);
					tmp = HBaseUtil.uniqAddHBaseCol(log, res, update.ib,
							FAMILY, COL_IB);
					byte[] ibVal = tmp.getBytes();
					long iuv = tmp.cardinality();
					byte[] iuvVal = Bytes.toBytes(iuv);
					long ar = iuv == 0 ? 0 : (int) ((double) auv / iuv
							* 10000.0 + 0.5);
					byte[] arVal = Bytes.toBytes(ar);
					long az = tn == 0 ? 0
							: (int) ((double) atn / tn * 10000.0 + 0.5);
					byte[] azVal = Bytes.toBytes(az);
					long au = iuv == 0 ? 0
							: (int) ((double) amt / iuv * 100.0 + 0.5);
					byte[] auVal = Bytes.toBytes(au);

					Put put = new Put(row);
					put.add(FAMILY, COL_PV, pvVal);
					put.add(FAMILY, COL_AMT, amtVal);
					put.add(FAMILY, COL_AN, anVal);
					put.add(FAMILY, COL_TN, tnVal);
					put.add(FAMILY, COL_ATN, atnVal);
					put.add(FAMILY, COL_AUV, auvVal);
					put.add(FAMILY, COL_IUV, iuvVal);
					put.add(FAMILY, COL_AR, arVal);
					put.add(FAMILY, COL_AZ, azVal);
					put.add(FAMILY, COL_AU, auVal);
					put.add(FAMILY, COL_AB, abVal);
					put.add(FAMILY, COL_IB, ibVal);

					puts.add(put);
				}

				tabShopItemHourly.put(puts);
				tabShopItemHourly.flushCommits();

				log.info(String.format("Merged %d records into %s table",
						puts.size(), Constants.DP_H_TABLE));
			} catch (Exception e) {
				log.error(String.format("Failed to merge into %s table",
						Constants.DP_H_TABLE), e);
			}

			if (tabShopItemHourly != null) {
				try {
					tabShopItemHourly.close();
				} catch (Exception e) {
					log.error("Failed to return HTable to pool", e);
				}
			}
		}

		void mergeWriteDaily(List<Get> gets, List<UnitItem> updates) {
			HTableInterface tabShopItemDaily = null;
			List<Put> puts = new ArrayList<Put>();
			AdaptiveCounting tmp = null;

			try {
				tabShopItemDaily = poolRef.getTable(Constants.DP_D_TABLE);

				Result[] results = tabShopItemDaily.get(gets);
				for (int i = 0; i < results.length; i++) {
					Result res = results[i];
					UnitItem update = updates.get(i);
					byte[] row = gets.get(i).getRow();

					byte[] pvVal = HBaseUtil.addHBaseCol(log, res, update.pv,
							FAMILY, COL_PV);
					byte[] amtVal = HBaseUtil.addHBaseCol(log, res,
							update.alipayAmt, FAMILY, COL_AMT);
					long amt = Bytes.toLong(amtVal);
					byte[] anVal = HBaseUtil.addHBaseCol(log, res,
							update.alipayNum, FAMILY, COL_AN);
					byte[] tnVal = HBaseUtil.addHBaseCol(log, res,
							update.tradeNum, FAMILY, COL_TN);
					long tn = Bytes.toLong(tnVal);
					byte[] atnVal = HBaseUtil.addHBaseCol(log, res,
							update.alipayTradeNum, FAMILY, COL_ATN);
					long atn = Bytes.toLong(atnVal);
					tmp = HBaseUtil.uniqAddHBaseCol(log, res, update.ab,
							FAMILY, COL_AB);
					byte[] abVal = tmp.getBytes();
					long auv = tmp.cardinality();
					byte[] auvVal = Bytes.toBytes(auv);
					tmp = HBaseUtil.uniqAddHBaseCol(log, res, update.ib,
							FAMILY, COL_IB);
					byte[] ibVal = tmp.getBytes();
					long iuv = tmp.cardinality();
					byte[] iuvVal = Bytes.toBytes(iuv);
					long ar = iuv == 0 ? 0 : (int) ((double) auv / iuv
							* 10000.0 + 0.5);
					byte[] arVal = Bytes.toBytes(ar);
					long az = tn == 0 ? 0
							: (int) ((double) atn / tn * 10000.0 + 0.5);
					byte[] azVal = Bytes.toBytes(az);
					long au = iuv == 0 ? 0
							: (int) ((double) amt / iuv * 100.0 + 0.5);
					byte[] auVal = Bytes.toBytes(au);

					Put put = new Put(row);
					put.add(FAMILY, COL_PV, pvVal);
					put.add(FAMILY, COL_AMT, amtVal);
					put.add(FAMILY, COL_AN, anVal);
					put.add(FAMILY, COL_TN, tnVal);
					put.add(FAMILY, COL_ATN, atnVal);
					put.add(FAMILY, COL_AUV, auvVal);
					put.add(FAMILY, COL_IUV, iuvVal);
					put.add(FAMILY, COL_AR, arVal);
					put.add(FAMILY, COL_AZ, azVal);
					put.add(FAMILY, COL_AU, auVal);
					put.add(FAMILY, COL_AB, abVal);
					put.add(FAMILY, COL_IB, ibVal);

					puts.add(put);
				}

				tabShopItemDaily.put(puts);
				tabShopItemDaily.flushCommits();

				log.info(String.format("Merged %d records into %s table",
						puts.size(), Constants.DP_D_TABLE));
			} catch (Exception e) {
				log.error(String.format("Failed to merge into %s table",
						Constants.DP_D_TABLE), e);
			}

			if (tabShopItemDaily != null) {
				try {
					tabShopItemDaily.close();
				} catch (Exception e) {
					log.error("Failed to return HTable to pool", e);
				}
			}
		}
	}

}
