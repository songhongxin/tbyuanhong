package com.etao.lz.storm;

import gnu.trove.map.hash.TLongLongHashMap;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.etao.lz.star.StarLogProtos;
import com.etao.lz.star.StarLogProtos.FlowStarLog;
import com.etao.lz.star.StarLogProtos.FlowStarLog.Builder;
import com.etao.lz.storm.utils.GZipUtil;
import com.google.protobuf.InvalidProtocolBufferException;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.recipes.cache.ChildData;
import com.netflix.curator.framework.recipes.cache.PathChildrenCache;
import com.netflix.curator.framework.recipes.cache.PathChildrenCacheEvent;
import com.netflix.curator.framework.recipes.cache.PathChildrenCacheListener;
import com.netflix.curator.framework.recipes.cache.PathChildrenCacheMode;
import com.netflix.curator.retry.RetryNTimes;

/**
 * 流量日志读取Spout
 * 
 * 本任务不需要输入！
 * 
 * 本任务输出 tuple 字段及含义如下：
 * <ul>
 * <li>seller_id - String, 用于数据分区的id</li>
 * <li>log - byte[], ProtoBuffer 序列化后的流量日志记录</li>
 * </ul>
 * 
 * @author yuanhong.shx
 * 
 */
public class TrafficSpout extends BaseRichSpout {

	// 序列化ID号

	private static final long serialVersionUID = 1569168594203942609L;

	// 日志操作记录对象
	private static final Log LOG = LogFactory.getLog(TrafficSpout.class);

	// Spout输出对象
	private transient SpoutOutputCollector collector;

	// Tuple队列
	private LinkedBlockingQueue<Values> queue;

	// 扫描数据的起始时间，0表示未设定
	private int startTimestamp;

	// 扫描数据的截至时间，0表示未设定
	private int endTimestamp;

	// 是否启用GZip压缩
	private boolean enableCompress;

	// 统计输出相关配置
	private int outputModePerShard;
	private int outputModePerSpout;

	// 获取业务日志的HBase各个Shard分区的处理同步时间
	private Map<Short, Integer> syncTimestampMap;
	private int syncTsInterval;

	// Zookeeper客户端
	private transient CuratorFramework zkClient;
	private String zkQuorum;
	private int zkRetryTimes;
	private int zkRetrySleepInterval;

	// Spout工作状态
	private boolean spoutActiveStatus;

	// 统计相关计数指标
	private long emitFlowLogNum;
	private long failFlowLogNum;
	private long lastEmitOutputTime;

	// 日志发送延迟时间和日志记录间隔条数
	private long sleepTime = 0L;
	private long sleepRecord = 0L;

	// 店铺id->卖家id映射表
	private TLongLongHashMap shopId2SellerIdMap;

	transient PathChildrenCache spoutStatusZkCache;

	private transient TTSpoutMonitor monitor; // 监控

	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf,
			TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;

		// 吸星大法监控信息
		String name = MbConstants.TRAFFIC_SPOUT_ID;
		monitor = new TTSpoutMonitor(name, conf, context);
		monitor.reset();
		monitor.open();

		String propPath = "storm/bolt-config.properties"; // 固定写了

		initParameters(propPath);

		String sellerIdPath = "shop_seller_id_dim.ser";

		loadShopSellerIdMap(sellerIdPath);

		startZookeeperClient();

		addSpoutStatusListener();

		final List<Short> shardingKeyList = new ArrayList<Short>();
		startShardThread(context, shardingKeyList);

		startSyncThread(shardingKeyList);

		initStatCounters();

	}

	@Override
	public void nextTuple() {
		Values tuple;
		try {
			// Storm 3.x 对于同一 worker 内的多个 Spout task 是顺序调用其 nextTuple()
			// 方法的，若其中某个 task 的 nextTuple() 方法阻塞则后续 task 就无法被执行，故这里用 poll() 替代
			// take() 以避免长时间阻塞
			while ((tuple = queue.poll()) != null) {
				collector.emit(MbConstants.TRAFFIC_STREAMID, tuple);

				// 监控
				long ts = (Long) tuple.get(2);
				monitor.sign(ts);

				if (++emitFlowLogNum % outputModePerSpout == 0) {
					LOG.info("emitFlowLogNum:"
							+ emitFlowLogNum
							+ ", failFlowLogNum:"
							+ failFlowLogNum
							+ ", average emit qps:"
							+ (outputModePerSpout * 1000 / (System
									.currentTimeMillis() - lastEmitOutputTime))
							+ ", queue size:" + queue.size());
					lastEmitOutputTime = System.currentTimeMillis();
				}
			}
		} catch (Exception e) {
			LOG.error("failed to get tuple from blocking queue", e);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(MbConstants.TRAFFIC_STREAMID, new Fields(
				"seller_id", "log", "ts"));
	}

	/**
	 * 负责单个 shard的扫描和发送操作
	 * 
	 */
	private class ShardScanner implements Runnable {

		// 吸星大法HBase表browse_log的数据生成类
		private final NHbaseGenerator browseLogGenerator;
		// 吸星大法HBase表的sharding key，最多切分32 个分区
		private final short shardingKey;

		// 最近一次扫描的HBase表的时间戳 timestamp
		private int retimestamp;

		// 每次在lastRowKey后追加的byte，以避免重复获取该记录或产生潜在的丢失记录风险
		// private final byte[] pad = new byte[] { 0 };

		// 最近一次统计输出的时间戳
		private long lastScanOutputTime;

		// 数据源日志延迟后，暂停的时间次数
		private int ttdelayflag = 0;

		public ShardScanner(short shardingKey) {
			this(shardingKey, startTimestamp);
		}

		public ShardScanner(short shardingKey, int specifiedStartTs) {
			this.shardingKey = shardingKey;
			this.retimestamp = 0;

			// 吸星大法流量日志扫描类
			this.browseLogGenerator = new NHbaseGenerator(
					MbConstants.XXDF_HBASE_FLOW_TABLE, specifiedStartTs,
					endTimestamp);

			// 初始化最近统计输出的时间戳
			this.lastScanOutputTime = System.currentTimeMillis();
		}

		@Override
		public void run() {
			long totalCount = 0L;
			while (!Thread.interrupted()) {
				if (!spoutActiveStatus) {
					try {
						Thread.sleep(500);
					} catch (InterruptedException e) {
						LOG.error("thread sleep interrupted", e);
					}
					continue;
				}
				// Step 1) 按照shardingKey，从上次扫描截至的lastRowKey开始扫描日志
				ResultScanner rs = browseLogGenerator.scanMainTableTs(
						shardingKey, retimestamp);
				// Step 2) 遍历ResultScanner提取并发送日志，同时更新lastRowKey
				totalCount = extract(rs, totalCount);
			}
		}

		/**
		 * 遍历ResultScanner提取并发送日志
		 * 
		 * @param rs
		 * @param totalCount
		 * @return
		 */
		private long extract(ResultScanner rs, long totalCount) {
			Result rr = new Result();
			byte[] rowkey = null;
			byte[] value = null;
			StarLogProtos.FlowStarLog browseLog = null;
			if (rs != null) { // XXX: 获取Scanner成功，遍历处理
				while (rr != null) {
					try {
						rr = rs.next();
					} catch (IOException e) {
						LOG.error("scanner I/O error(" + shardingKey + ")", e);

						/*
						 * // 记录当前扫描到的rowkey，设置为lastRowKey，下次从该位置接着扫描 if (rowkey
						 * != null) lastRowKey = Bytes.add(rowkey, pad); rs =
						 * reOpenScanner(1000L, rs); rr = new Result();
						 * continue;
						 */
						break;

					}
					if (rr == null || rr.isEmpty())
						continue;
					rowkey = rr.getRow();
					/*
					 * byte[] tail = Bytes.tail(rowkey, rowkey.length - 5);
					 * byte[] puid = Bytes.head(tail, tail.length - 4);
					 */
					value = rr.getValue(
							Bytes.toBytes(MbConstants.XXDF_HBASE_FAMILY),
							Bytes.toBytes(MbConstants.XXDF_HBASE_PB_COL));

					// Step 2.1) 反序列化PB格式的日志记录
					try {
						browseLog = StarLogProtos.FlowStarLog.parseFrom(value);
					} catch (InvalidProtocolBufferException e1) {
						LOG.error("protobuf parse failed", e1);
						browseLog = null;
					}
					if (browseLog == null)
						continue;

					// 过滤掉宝贝id为空的流量日志
					long auction_id = 0;
					if (browseLog.hasShopid()
							&& !"".equals(browseLog.getAuctionid())) {
						try {
							auction_id = Long.parseLong(browseLog
									.getAuctionid());
						} catch (NumberFormatException e) {
							LOG.error("shop id format error", e);
						}
					}

					if (auction_id == 0)
						continue;

					long shopId = 0;
					if (browseLog.hasShopid()
							&& !"".equals(browseLog.getShopid())) {
						try {
							shopId = Long.parseLong(browseLog.getShopid());
						} catch (NumberFormatException e) {
							LOG.error("shop id format error", e);
						}
					} else {
						continue;
					}

					// 将流量日志中的shopid不在预估店铺中的过滤掉
					long seller_id = 0;
					if (shopId != 0 && shopId2SellerIdMap != null
							&& shopId2SellerIdMap.contains(shopId)) {
						seller_id = shopId2SellerIdMap.get(shopId);

					}
					if (seller_id == 0)
						continue;

					long ts = browseLog.getTs();

					// Step 2.2) 清空该日志记录中无用的字段
					byte[] newValue = resetLog(browseLog, seller_id);

					// Step 2.3) 发送该日志记录
					try {
						queue.put(new Values(seller_id, newValue, ts));
					} catch (InterruptedException e) {
						LOG.error("failed to put data to queue", e);
						failFlowLogNum++;
					}

					// Step 2.4) 更新日志处理时间戳
					int nowLogTs = (int) (browseLog.getTs() / 1000);
					Integer syncTs = syncTimestampMap.get(shardingKey);
					if ((syncTs == null)
							|| (syncTs != null && nowLogTs > syncTs.intValue())) {
						syncTimestampMap.put(shardingKey, nowLogTs);
					}

					// Step 2.5) 输出统计信息
					if (++totalCount % outputModePerShard == 0) {
						LOG.info("current log timestamp of shard("
								+ shardingKey
								+ "):"
								+ nowLogTs
								+ ", average scan qps:"
								+ (System.currentTimeMillis() - lastScanOutputTime));
						lastScanOutputTime = System.currentTimeMillis();
					}
					if (sleepRecord > 0 && sleepTime > 0
							&& (totalCount % sleepRecord == 0)) {
						try {
							Thread.sleep(sleepTime);
						} catch (InterruptedException e) {
							LOG.error("thread sleep interrupted", e);
						}
					}
				}
				rs.close();
			} else { // XXX: 获取Scanner时出现异常，休眠1s后重试
				try {
					Thread.sleep(3000L);
				} catch (InterruptedException e) {
					LOG.error("thread sleep interrupted", e);
				}
			}

			// 更新最近扫描到的rowkey为从本次扫描到的最后一条记录的rowkey，或当本次扫描未得到记录时则更新为本次setStopRow设置的rowkey
			// byte[] bShard = { (byte) shardingKey };

			if (rowkey == null) {
				ttdelayflag++;

				try {
					Thread.sleep(3000L); // 停止10s----压测时1秒
					LOG.info("stop scanning , no data  has been written recently! stop times : "
							+ ttdelayflag);

					LOG.info("now try to scan the log : nowshard : "
							+ shardingKey
							+ " nowstarttime : "
							+ retimestamp
							+ " nowtime : "
							+ (retimestamp % MbConstants.XXDF_HBASE_TIME_INTERVAL));

				} catch (InterruptedException e) {
					LOG.error(
							"sleeping  for 2 minitue agao  no data  has been written!",
							e);
				}

			} else {

				ttdelayflag = 0;
			}

			int lastScanTs = browseLogGenerator.getLastTimestamp();
			/*
			 * lastRowKey = (rowkey != null) ? Bytes.add(rowkey, pad) :
			 * Bytes.add( bShard, Bytes.toBytes(lastScanTs %
			 * Constants.XXDF_HBASE_TIME_INTERVAL));
			 */
			retimestamp = lastScanTs;
			return totalCount;
		}

		/**
		 * 清空日志记录中无用的字段
		 * 
		 * @param flowLog
		 * @return
		 */
		private byte[] resetLog(FlowStarLog flowLog, long seller_id) {
			Builder builder = StarLogProtos.FlowStarLog.newBuilder();
			builder.setTs(flowLog.getTs());
			/*
			 * builder.setUrl(flowLog.getUrl());
			 * builder.setReferUrl(flowLog.getReferUrl());
			 * builder.setUidMid(flowLog.getUidMid()); long shopId = 0; if
			 * (flowLog.hasShopid() && !"".equals(flowLog.getShopid())) { try {
			 * shopId = Long.parseLong(flowLog.getShopid()); } catch
			 * (NumberFormatException e) { LOG.error("shop id format error", e);
			 * } } // 将流量日志中的shopid映射为sellerid后填充到shopid字段 if (shopId != 0 &&
			 * shopId2SellerIdMap != null &&
			 * shopId2SellerIdMap.contains(shopId)) {
			 * builder.setShopid(String.valueOf
			 * (shopId2SellerIdMap.get(shopId))); } else {
			 * builder.setShopid(flowLog.getShopid()); }
			 * builder.setAuctionid(flowLog.getAuctionid());
			 * builder.setUid(flowLog.getUid());
			 * builder.setPuid(flowLog.getPuid());
			 */
			builder.setShopid(String.valueOf(seller_id));
			// builder.setMid(flowLog.getMid());
			builder.setUidMid(flowLog.getUidMid());
			builder.setAuctionid(flowLog.getAuctionid());
			byte[] newValue = (enableCompress ? GZipUtil.GZip(builder.build()
					.toByteArray()) : builder.build().toByteArray());
			return newValue;
		}

		/**
		 * 关闭后重新打开一个ResultScanner，应对ResultScanner超时等IO异常
		 * 
		 * @param sleepTime
		 * @param rs
		 */
		@SuppressWarnings("unused")
		private ResultScanner reOpenScanner(long sleepTime, ResultScanner rs) {
			rs.close();
			ResultScanner newRs = null;
			final int[] sleepIntervals = { 1, 2, 4, 6, 6, 6, 8, 8, 9, 10 };
			int retryTimes = 0;
			long totalSleepTime = 0;
			do {
				try {
					Thread.sleep(sleepTime
							* sleepIntervals[retryTimes % sleepIntervals.length]);
				} catch (InterruptedException e) {
					LOG.error("thread sleep interrupted", e);
				}
				newRs = browseLogGenerator.scanMainTableTs(shardingKey,
						retimestamp);
				totalSleepTime += sleepTime
						* sleepIntervals[retryTimes % sleepIntervals.length];
				retryTimes++;
			} while (newRs == null);

			LOG.warn("scanner error occured(" + shardingKey
					+ "), close and open a new scanner after sleeping "
					+ totalSleepTime + "ms, retry " + retryTimes + " times");
			return newRs;
		}
	}

	/**
	 * 初始化从配置文件加载的参数
	 * 
	 * @param propPath
	 */
	private void initParameters(String propPath) {
		final PropConfig pc;
		try {
			pc = new PropConfig(propPath);
		} catch (IOException e1) {
			LOG.error("failed to load poperties", e1);
			throw new RuntimeException(e1);
		}
		int queueSize = 1000;
		if (pc.getProperty(MbConstants.TRAFFIC_SPOUT_QUEUE_SIZE) != null)
			queueSize = Integer.valueOf(pc
					.getProperty(MbConstants.TRAFFIC_SPOUT_QUEUE_SIZE));
		this.queue = new LinkedBlockingQueue<Values>(queueSize);

		String startTs = pc
				.getProperty(MbConstants.TRAFFIC_SPOUT_START_TIMESTAMP);
		if (startTs != null) { // 从指定时间开始扫描
			this.startTimestamp = Integer.valueOf(startTs);
		} else { // 按默认配置扫描
			this.startTimestamp = 0;
		}

		String endTs = pc.getProperty(MbConstants.TRAFFIC_SPOUT_END_TIMESTAMP);
		if (endTs != null) { // 从指定时间开始扫描
			this.endTimestamp = Integer.valueOf(endTs);
		} else { // 按默认配置扫描
			this.endTimestamp = 0;
		}

		this.enableCompress = false;
		String enableCompressString = pc
				.getProperty(MbConstants.TRAFFIC_SPOUT_ENABLE_COMPRESS);
		if (enableCompressString != null && enableCompressString.equals("1")) {
			this.enableCompress = true;
		}

		zkQuorum = "localhost:2181";
		if (pc.getProperty(MbConstants.ZK_SERVERS) != null)
			zkQuorum = pc.getProperty(MbConstants.ZK_SERVERS);

		zkRetryTimes = 1000;
		if (pc.getProperty(MbConstants.ZK_RETRY_TIMES) != null)
			zkRetryTimes = Integer.valueOf(pc
					.getProperty(MbConstants.ZK_RETRY_TIMES));

		zkRetrySleepInterval = 100;
		if (pc.getProperty(MbConstants.ZK_RETRY_SLEEP_INTERVAL) != null)
			zkRetrySleepInterval = Integer.valueOf(pc
					.getProperty(MbConstants.ZK_RETRY_SLEEP_INTERVAL));

		outputModePerShard = 100000;
		if (pc.getProperty(MbConstants.TRAFFIC_SPOUT_OUTPUT_MODE_PER_SHARD) != null)
			outputModePerShard = Integer
					.valueOf(pc
							.getProperty(MbConstants.TRAFFIC_SPOUT_OUTPUT_MODE_PER_SHARD));

		outputModePerSpout = 1000000;
		if (pc.getProperty(MbConstants.TRAFFIC_SPOUT_OUTPUT_MODE_PER_SPOUT) != null)
			outputModePerSpout = Integer
					.valueOf(pc
							.getProperty(MbConstants.TRAFFIC_SPOUT_OUTPUT_MODE_PER_SPOUT));

		syncTsInterval = 1;
		if (pc.getProperty(MbConstants.TRAFFIC_SPOUT_SYNC_TS_INTERVAL) != null)
			syncTsInterval = Integer.valueOf(pc
					.getProperty(MbConstants.TRAFFIC_SPOUT_SYNC_TS_INTERVAL));
	}

	/**
	 * 加载shopId到sellerId的映射表
	 * 
	 * @param sellerIdPath
	 */
	private void loadShopSellerIdMap(String sellerIdPath) {
		ObjectInputStream ois;
		try {
			if (sellerIdPath != null) {
				InputStream is = ClassLoader
						.getSystemResourceAsStream(sellerIdPath);
				if (is != null) {
					ois = new ObjectInputStream(is);
					shopId2SellerIdMap = (TLongLongHashMap) ois.readObject();
					ois.close();
					is.close();
				} else {
					LOG.error("fail to load shop seller id file: "
							+ sellerIdPath);
					return;
				}
			} else {
				LOG.warn("have not set shop seller id file!");
				return;
			}
		} catch (ClassNotFoundException e) {
			LOG.error("load shop seller id class not found", e);
		} catch (IOException e) {
			LOG.error("load shop seller id IO exception", e);
		}
		if (shopId2SellerIdMap != null) {
			LOG.info("finish to load shop seller id, total count: "
					+ shopId2SellerIdMap.size());
			LOG.debug("shop id to seller id map: "
					+ shopId2SellerIdMap.toString());
		}
	}

	/**
	 * 启动Zookeeper客户端连接
	 */
	private void startZookeeperClient() {
		try {
			zkClient = CuratorFrameworkFactory
					.builder()
					.connectString(zkQuorum)
					.retryPolicy(
							new RetryNTimes(zkRetryTimes, zkRetrySleepInterval))
					.build();
			zkClient.start();
		} catch (IOException e1) {
			LOG.error("failed to connect to zookeeper", e1);
		}
	}

	/**
	 * 添加Zookeeper监听目录，同步Spout工作状态
	 */
	private void addSpoutStatusListener() {
		spoutStatusZkCache = new PathChildrenCache(zkClient,
				MbConstants.ZK_SYNC_PREFIX.substring(0,
						MbConstants.ZK_SYNC_PREFIX.length() - 1),
				PathChildrenCacheMode.CACHE_DATA);
		PathChildrenCacheListener spoutListener = new PathChildrenCacheListener() {
			@Override
			public void childEvent(CuratorFramework client,
					PathChildrenCacheEvent event) throws Exception {
				switch (event.getType()) {
				case CHILD_ADDED:
				case CHILD_UPDATED:
					// NOTE: 仅在 CHILD_ADDED/UPDATED/REMOVED 等事件时 ChildData 才不为
					// null
					ChildData childData = event.getData();
					String path = childData.getPath();
					byte[] data = childData.getData();
					if ((MbConstants.ZK_CONF_PREFIX + MbConstants.ZK_SYNC_ACTIVE_TRAFFIC_SPOUT)
							.equals(path) && data != null) {
						spoutActiveStatus = (data[0] != 0) ? true : false;
						LOG.info("zookeeper node change: active_traffic_spout="
								+ spoutActiveStatus);
					} else if ((MbConstants.ZK_CONF_PREFIX + MbConstants.ZK_SYNC_TRAFFIC_SPOUT_SLEEP_RECORD)
							.equals(path) && data != null) {
						sleepRecord = Bytes.toLong(data);
						LOG.info("zookeeper node change: traffic_spout_sleep_record="
								+ sleepRecord);
					} else if ((MbConstants.ZK_CONF_PREFIX + MbConstants.ZK_SYNC_TRAFFIC_SPOUT_SLEEP_TIME)
							.equals(path) && data != null) {
						sleepTime = Bytes.toLong(data);
						LOG.info("zookeeper node change: traffic_spout_sleep_time="
								+ sleepTime);
					}
					break;
				default:
					break;
				}
			}
		};
		spoutStatusZkCache.getListenable().addListener(spoutListener);
		try {
			spoutStatusZkCache.start();
		} catch (Exception e) {
			LOG.error("failed to start traffic spout status zk listener", e);
		}
	}

	/**
	 * 启动该任务负责处理的Shard扫描线程
	 * 
	 * @param context
	 * @param shardingKeyList
	 */
	private void startShardThread(TopologyContext context,
			final List<Short> shardingKeyList) {
		this.spoutActiveStatus = true;
		// 根据 task_id 决定当前任务负责扫描哪些 HBase 区块
		int taskId = context.getThisTaskId();
		String componentId = context.getComponentId(taskId);
		int taskNum = context.getComponentTasks(componentId).size();
		int rem = taskId % taskNum;
		syncTimestampMap = new ConcurrentHashMap<Short, Integer>(
				MbConstants.XXDF_HBASE_SHARDING_NUM / taskNum);
		try {
			for (short i = 0; i < MbConstants.XXDF_HBASE_SHARDING_NUM; i++) {
				if (i % taskNum == rem) {
					String shardZkPath = MbConstants.ZK_SYNC_PREFIX + i;
					if (zkClient.checkExists().forPath(shardZkPath) == null) { // 创建zk节点
						zkClient.create().creatingParentsIfNeeded()
								.forPath(shardZkPath, null);
						Thread scanThread = new Thread(new ShardScanner(i));
						scanThread.setDaemon(true);
						scanThread.start();
					} else { // 从zk节点读取上次发送时间戳
						byte[] shardZkData = zkClient.getData().forPath(
								shardZkPath);
						Thread scanThread = null;
						if (shardZkData == null) {
							scanThread = new Thread(new ShardScanner(i));
						} else {
							int zkRecoverTs = Bytes.toInt(shardZkData);
							scanThread = new Thread(new ShardScanner(i,
									zkRecoverTs));
						}
						scanThread.setDaemon(true);
						scanThread.start();
					}
					shardingKeyList.add(i);
				}
			}
		} catch (Exception e) {
			LOG.error("failed to create scan thread", e);
			throw new RuntimeException(e);
		}
	}

	/**
	 * 启动流量和业务日志处理时间戳同步线程
	 * 
	 * @param shardingKeyList
	 */
	private void startSyncThread(final List<Short> shardingKeyList) {
		// 创建后台线程为各个shard线程同步业务日志处理时间
		Thread syncThread = new Thread(new Runnable() {
			@Override
			public void run() {
				String shardZkPath = null;
				while (!Thread.interrupted()) {
					for (short shardingKey : shardingKeyList) {
						shardZkPath = MbConstants.ZK_SYNC_PREFIX + shardingKey;
						try {
							if (zkClient.checkExists().forPath(shardZkPath) != null) {
								if (!syncTimestampMap.containsKey(shardingKey))
									continue;
								int syncTs = syncTimestampMap.get(shardingKey);
								zkClient.setData().forPath(shardZkPath,
										Bytes.toBytes(syncTs));
							}
						} catch (Exception e) {
							LOG.error("failed to set data to zookeeper", e);
						}
					}
					try {
						Thread.sleep(syncTsInterval * 1000L);
					} catch (InterruptedException e) {
						LOG.error("thread sleep interrupted", e);
					}
				}
			}
		});
		syncThread.setDaemon(true);
		syncThread.start();
	}

	/**
	 * 初始化计数器
	 */
	private void initStatCounters() {
		this.emitFlowLogNum = 0L;
		this.failFlowLogNum = 0L;
		this.lastEmitOutputTime = System.currentTimeMillis();
	}

	/*
	 * 吸星大法监控
	 */
	private final class TTSpoutMonitor extends AppMonitor {

		private static final long serialVersionUID = 2694180049179434218L;

		@SuppressWarnings("rawtypes")
		public TTSpoutMonitor(String name, Map conf, TopologyContext context) {
			super(name, conf, context);
			// TODO Auto-generated constructor stub
		}

		@Override
		protected int getQueueSize() {
			return queue.size();
		}

	}

}
