package com.etao.lz.storm;

import gnu.trove.map.hash.TLongLongHashMap;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.RotatingMap;

import com.etao.lz.star.StarLogProtos;
import com.etao.lz.star.StarLogProtos.BusinessStarLog;
import com.etao.lz.star.StarLogProtos.BusinessStarLog.Builder;
import com.etao.lz.storm.utils.GZipUtil;
import com.etao.lz.storm.utils.TimeUtil;
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
 * 业务日志读取Spout
 * 
 * 本任务不需要输入！
 * 
 * 本任务输出 tuple 字段及含义如下：
 * <ul>
 * <li>puid - String, 用于数据分区的伪 uid</li>
 * <li>log - byte[], ProtoBuffer 序列化后的成交日志记录</li>
 * </ul>
 * 
 * @author jiuling.ypf
 * 
 * 
 * 
 * 
 */
public class BusinessSpout extends BaseRichSpout {

	// 序列化ID号

	private static final long serialVersionUID = -6214939627253649913L;

	// 日志操作记录对象
	private static final Log LOG = LogFactory.getLog(BusinessSpout.class);

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

	// 设置业务日志的HBase各个Shard分区的处理同步时间
	private Map<Short, Integer> syncTimestampMap;
	private int syncTsInterval;

	// 设置业务日志处理时间与流量日志处理时间延迟时间间隔
	private int syncTsLatency;

	// Zookeeper客户端
	private transient CuratorFramework zkClient;
	private String zkQuorum;
	private int zkRetryTimes;
	private int zkRetrySleepInterval;

	// Spout工作状态
	private boolean spoutActiveStatus;

	// 统计相关计数指标
	private long discardOrderNum;
	private long emitGmtOrderNum;
	private long failGmtOrderNum;
	private long emitPayOrderNum;
	private long failPayOrderNum;
	private long emitBizOrderNum;
	private long lastEmitOutputTime;

	// 日志发送延迟时间和日志记录间隔条数
	private long sleepTime = 0L;
	private long sleepRecord = 0L;

	// 卖家id->店铺id映射表
	private TLongLongHashMap SellerId2shopIdMap;
	transient PathChildrenCache spoutStatusZkCache;

	private transient TTSpoutMonitor monitor; // 监控

	@Override
	public void open(@SuppressWarnings("rawtypes") Map conf,
			TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;

		// 吸星大法监控信息
		String name = MbConstants.BUSINESS_SPOUT_ID;
		monitor = new TTSpoutMonitor(name, conf, context);
		monitor.reset();
		monitor.open();

		String propPath = "storm/bolt-config.properties"; // 固定写了

		initParameters(propPath);

		String sellerIdPath = "seller_shop_id_dim.ser";

		loadShopSellerIdMap(sellerIdPath);

		// String propPath = (String) conf
		// .get(Constants.STORM_CONF_TASK_PROPERTY_PATH);
		initParameters(propPath);

		startZookeeperClient();

		// 通过zk里面的spout状态， 来控制是否继续扫描发送
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
				collector.emit(MbConstants.BIZ_STREAMID, tuple);

				// 监控
				long ts = (Long) tuple.get(2);
				monitor.sign(ts);

				if (++emitBizOrderNum % 1000 == 0) {
					LOG.info("discardOrderNum:" + discardOrderNum
							+ ", emitGmtOrderNum:" + emitGmtOrderNum
							+ ", failGmtOrderNum:" + failGmtOrderNum
							+ ", emitPayOrderNum:" + emitPayOrderNum
							+ ", failPayOrderNum:" + failPayOrderNum);
					LOG.info("emitBizOrderNum:"
							+ emitBizOrderNum
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
		declarer.declareStream(MbConstants.BIZ_STREAMID, new Fields("seller_id",
				"log", "ts"));
	}

	/**
	 * 负责单个 shard的扫描和发送操作
	 * 
	 */
	private class ShardScanner implements Runnable {

		// 吸星大法HBase表biz_order的数据生成类
		private final HBaseGenerator bizOrderGenerator;
		// 吸星大法HBase表biz_order_index的数据生成类
		private final HBaseGenerator bizOrderIdxGenerator;
		// 吸星大法HBase表pay_order的数据生成类
		private final HBaseGenerator payOrderGenerator;
		// 吸星大法HBase表pay_order_index的数据生成类
		private final HBaseGenerator payOrderIdxGenerator;
		// 吸星大法HBase表的sharding key，最多切分32 个分区
		private final short shardingKey;
		// 最近一次扫描的HBase表的行rowkey
		private byte[] lastRowKey;

		// 每次在lastRowKey后追加的byte，以避免重复获取该记录或产生潜在的丢失记录风险
		private final byte[] pad = new byte[] { 0 };

		// 最近一次统计输出的时间戳
		private long lastScanOutputTime;

		// 数据源日志延迟后，暂停的时间次数
		// 数据源日志延迟的标志，当为1的时候，证明日志延迟了，恢复后不能立即扫描，需要等待数据写入一段时间再扫描
		private int ttdelayflag = 0;

		// 缓存biz_order表中未发射的子订单
		private final RotatingMap<ByteArrayWrapper, StarLogProtos.BusinessStarLog> pendingChildBizOrderMap;
		private final static long rotateIntervalTime = 20 * 1000L;
		private long lastRotateTime;
		
	//	private HashMap<String, StarLogProtos.BusinessStarLog> pendingfatherBizOrderMap2;     // add by yuanhong.shx

		public ShardScanner(short shardingKey) {
			this(shardingKey, startTimestamp);
		}

		public ShardScanner(short shardingKey, int specifiedStartTs) {
			this.shardingKey = shardingKey;
			this.lastRowKey = null;

			this.bizOrderGenerator = new HBaseGenerator(
					MbConstants.XXDF_HBASE_BIZORDER_TABLE, specifiedStartTs,
					endTimestamp);
			this.bizOrderGenerator.setSyncTsLatency(syncTsLatency);
			this.bizOrderIdxGenerator = new HBaseGenerator(
					MbConstants.XXDF_HBASE_BIZORDER_IDX_TABLE, specifiedStartTs,
					endTimestamp);
			this.payOrderGenerator = new HBaseGenerator(
					MbConstants.XXDF_HBASE_PAYORDER_TABLE, specifiedStartTs,
					endTimestamp);
			this.payOrderIdxGenerator = new HBaseGenerator(
					MbConstants.XXDF_HBASE_PAYORDER_IDX_TABLE, specifiedStartTs,
					endTimestamp);

			this.lastScanOutputTime = System.currentTimeMillis();

			// 缓存订单的超时时间为：numBuckets * rotateIntervalTime = 60 seconds
			this.pendingChildBizOrderMap = new RotatingMap<ByteArrayWrapper, StarLogProtos.BusinessStarLog>(
					3);

			this.lastRotateTime = System.currentTimeMillis();
			
			//缓存父订单的信息
//			this.pendingfatherBizOrderMap2 =  new HashMap<String, StarLogProtos.BusinessStarLog>();  
			
			

		}

		@Override
		public void run() {
			long totalCount = 0L;
			final int MAX_PENDING_ORDER_COUNT_PER_SHARD = 10000;
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
				ResultScanner rs = bizOrderGenerator.scanMainTable(shardingKey,
						lastRowKey);

				// Step 2) 遍历ResultScanner提取日志，得到父子订单关系及子订单列表
				List<byte[]> childrenBizOrderKeyList = new ArrayList<byte[]>(); // 待关联查询的biz_order中的子订单列表
				TreeMap<String, Pair<List<byte[]>, String>> bizOrder2ChildrenBizOrderMap = new TreeMap<String, Pair<List<byte[]>, String>>(); // biz_order中父订单id到子订单rowkey的映射
				totalCount = extractChildrenBizOrder(rs, totalCount,
						bizOrder2ChildrenBizOrderMap, childrenBizOrderKeyList);

				// Step 3) 已经在内存中缓存的子订单，则不再查询HBase biz_order订单表
				List<byte[]> tempchildrenOrderKeyList = new ArrayList<byte[]>();
				for (byte[] childKey : childrenBizOrderKeyList) {
					if (!pendingChildBizOrderMap
							.containsKey(new ByteArrayWrapper(childKey))) {
						tempchildrenOrderKeyList.add(childKey);
					}
				}
				childrenBizOrderKeyList = tempchildrenOrderKeyList;

				// Step 4) 批量查询一次HBase biz_order订单表得到剩下的子订单信息
				if (childrenBizOrderKeyList.size() != 0) {
					getChildrenBizOrder(childrenBizOrderKeyList);
					childrenBizOrderKeyList.clear();
				}
				childrenBizOrderKeyList = null;

				// Step 5) 根据父子订单关系，拆分子订单并发送给下游Bolt
				if (bizOrder2ChildrenBizOrderMap.size() != 0) {
					// Step 5。1) 根据父订单号查询HBase pay_order订单表得到父订单总钱数
					Map<String, Long> payOrder2TotalFeeMap = getPayOrderTotalFee(bizOrder2ChildrenBizOrderMap);
					// Step 5。2) 根据父子订单记录以及父订单总钱数，拆分子订单并发送子订单记录
					splitAndSendChildrenBizOrder(bizOrder2ChildrenBizOrderMap,
							payOrder2TotalFeeMap);
					bizOrder2ChildrenBizOrderMap.clear();
					payOrder2TotalFeeMap.clear();
					payOrder2TotalFeeMap = null;
				}
				bizOrder2ChildrenBizOrderMap = null;

				// Step 6) 轮转淘汰过期的缓存biz_order订单
				long nowTime = System.currentTimeMillis();
				if (nowTime - lastRotateTime >= rotateIntervalTime
						|| pendingChildBizOrderMap.size() >= MAX_PENDING_ORDER_COUNT_PER_SHARD) {
					Map<ByteArrayWrapper, StarLogProtos.BusinessStarLog> dead = pendingChildBizOrderMap
							.rotate();
					dead.clear();
					dead = null;
					lastRotateTime = nowTime;
				}				
				
			}
		}

		/**
		 * 遍历ResultScanner提取日志，得到父子订单关系及子订单列表
		 * 
		 * @param rs
		 * @param totalCount
		 * @param bizOrder2ChildrenOrderMap
		 * @param childrenOrderKeyList
		 * @return
		 */
		private long extractChildrenBizOrder(
				ResultScanner rs,
				long totalCount,
				TreeMap<String, Pair<List<byte[]>, String>> bizOrder2ChildrenBizOrderMap,
				List<byte[]> childrenBizOrderKeyList) {
			if (rs == null) { // XXX: 获取Scanner时出现异常，休眠1s后重试
				try {
					Thread.sleep(1000L);
				} catch (InterruptedException e) {
					LOG.error("thread sleep interrupted", e);
				}
				return totalCount;
			}
			Result rr = new Result();
			byte[] rowkey = null;
			byte[] value = null;
			// 当前遍历的biz_order中的订单日志记录
			StarLogProtos.BusinessStarLog bizOrder = null;
			int nowLogTs = 0;

			short flagonce = 0; // 配合，延迟后立即扫描数据造成丢失的问题，继续等待20秒

			// 遍历biz_order记录，解析protobuf，完成子订单拆分
			while (rr != null) {
				try {
					rr = rs.next();
				} catch (IOException e) {
					LOG.error("scanner I/O error(" + shardingKey + ")", e);
					// 记录当前扫描到的rowkey，设置为lastRowKey，下次从该位置接着扫描
					if (rowkey != null)
						lastRowKey = Bytes.add(rowkey, pad);
					rs = reOpenScanner(1000L, rs);
					rr = new Result();
					continue;
				}
				if (rr == null || rr.isEmpty())
					continue;
				rowkey = rr.getRow();

				// ----------------------------------非业务逻辑代码
				if (ttdelayflag != 0 && rowkey != null) { // 发生在，延迟恢复后的第一次扫描

					flagonce = 1; // 局部变量，只需设置一次，后面不用重置。
					rowkey = null;
					break;
				}
				// ----------------------------------

				value = rr.getValue(Bytes.toBytes(MbConstants.XXDF_HBASE_FAMILY),
						Bytes.toBytes(MbConstants.XXDF_HBASE_PB_COL));

				try {
					bizOrder = StarLogProtos.BusinessStarLog.parseFrom(value);
				} catch (InvalidProtocolBufferException e) {
					LOG.error("protobuf parsing error", e);
					bizOrder = null;
				}
				if (bizOrder == null)
					continue;

				// XXX: 过滤掉分销类别的订单记录！！！
				int bizOrderType = bizOrder.getBizType();
				if (bizOrderType == 800)
					continue;

				// xxx : 过滤掉不在天猫报名的卖家！！
				String sellerid_s = bizOrder.getSellerId();
				if (sellerid_s.equals("\\N"))
					continue;

				long seller_id = 0;
				try {
					seller_id = Long.parseLong(sellerid_s);
				} catch (NumberFormatException e) {
					LOG.error("seller id format error", e);
				}
				if (seller_id == 0 || SellerId2shopIdMap == null
						|| !SellerId2shopIdMap.contains(seller_id))
					continue;

				int isPay = bizOrder.getIsPay();
				int isNewGenerated = bizOrder.getIsNewGenerated();

				// XXX: 以下两种情况下发送拍下和成交订单进行实时效果计算：
				// 1. is_new_generated=1为拍下订单；
				// 2. is_pay=1为成交订单（包含biz_type=200，10000等）。
				
				// XXX: 过滤掉天猫预售的未付全款的日志
				if (bizOrderType == 10000) {
					String attributes = bizOrder.getAttributes();
					String stepss = "";
					String[] attr = attributes.split(";", -1);
					for (String element : attr) {
						if (element.startsWith("stepss:")) {
							if (element.length() > 7)
								stepss = element.substring(7);
							break;
						}
					}
					if (!stepss.equals("2")){
						
					//	LOG.info("taiqi  atributes " + bizOrder.getAttributes());
						
						continue;
					
					}
					
					String dbAction = bizOrder.getDbAction();
					if (dbAction == null)
						continue;
					String[] items = dbAction.split(":", -1);
					if (items.length > 1) {
						String[] fields = items[1].split(",", -1);
						for (String field : fields) {
							if (field.equals("32")) {
								isPay = 1;
								bizOrder = resetPreLog(bizOrder);
							
							//	LOG.info("***found pre pay order: sellerid=" + bizOrder.getSellerId() + ", orderid=" + bizOrder.getOrderId());
								break;
							}
						}
					}
				}
				
				
				if (isNewGenerated != 1 && isPay != 1) {
					discardOrderNum++;
					continue;
				}

				long ts = bizOrder.getOrderModifiedT(); // 获取日志时间戳

				// XXX: 拍下订单不需要关联pay_order直接通过queue发送
				if (isNewGenerated == 1) {
					try {
						byte[] newValue = resetLog(bizOrder, 0, "");
						queue.put(new Values(seller_id, newValue, ts));
						emitGmtOrderNum++;
					} catch (InterruptedException e) {
						LOG.error("failed to put data to queue", e);
						failGmtOrderNum++;
					}
					continue;
				}
				// XXX: 成交订单需要关联pay_order后通过queue发送
				String orderId = bizOrder.getOrderId();
				int isDetail = bizOrder.getIsDetail();
				int isMain = bizOrder.getIsMain();
				List<byte[]> bizChildrenOrder = new ArrayList<byte[]>();

				
				if (isMain == 1 && isDetail == 1) { // Case 1:
													// 既是父订单又是子订单
					bizChildrenOrder.add(rowkey);
					pendingChildBizOrderMap.put(new ByteArrayWrapper(rowkey),
							bizOrder);
				} else if (isMain == 1 && isDetail == 0) { // Case2:
															// 只是父订单不是子订单
					// 根据父订单的order id查找索引表得到各个子订单的order id
					bizChildrenOrder = scanChildrenOrderId(orderId);
					childrenBizOrderKeyList.addAll(bizChildrenOrder);
				} else if (isMain == 0 && isDetail == 1) { // Case3:
															// 只是子订单不是父订单
					// 将子订单缓存起来，以便后续父订单关联子订单时不用重复从HBase查询已经得到的订单记录！
					pendingChildBizOrderMap.put(new ByteArrayWrapper(rowkey),
							bizOrder);
					lastRowKey = Bytes.add(rowkey, pad);
					continue;
				} else { // Case 4: 异常数据直接丢弃
					LOG.error("invalid biz_order log, drop it");
					lastRowKey = Bytes.add(rowkey, pad);
					continue;
				}

				
				// XXX: 过滤掉父订单pay_time为空的情况
				
				String payTime = bizOrder.getPayTime();
				/*
				if (payTime.equals("\\N"))
					continue;
                */
				
				bizOrder2ChildrenBizOrderMap.put(orderId,
						new Pair<List<byte[]>, String>(bizChildrenOrder,
								payTime));

				// 遍历过程中业务日志处理时间戳已快于流量日志处理时间戳，则休眠固定时间（500ms）
				nowLogTs = (int) (TimeUtil.bizTimeToTs(bizOrder) / 1000);
				Integer syncTs = syncTimestampMap.get(shardingKey);
				if (syncTs != null
						&& (syncTs.intValue() - nowLogTs) < syncTsLatency
						&& spoutActiveStatus) {
					try {
						Thread.sleep(500);
					} catch (InterruptedException e) {
						LOG.error("thread sleep interrupted", e);
					}
				}

				// 输出统计信息
				if (++totalCount % outputModePerShard == 0) {
					LOG.info("current log timestamp of shard(" + shardingKey
							+ "):" + nowLogTs + ", average scan qps:"
							+ (System.currentTimeMillis() - lastScanOutputTime)
							+ ", pending orders:"
							+ pendingChildBizOrderMap.size());
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

			// 同步流量和业务日志的处理时间戳
			Integer syncLogTs = syncTimestampMap.get(shardingKey);
			int lastScanTs = bizOrderGenerator.getLastTimestamp();
			int bizLogTimestamp = (rowkey != null) ? nowLogTs : lastScanTs;
			if (syncLogTs != null
					&& (syncLogTs.intValue() - bizLogTimestamp) < syncTsLatency) {
				int syncLogTimestamp = syncLogTs.intValue();
				int deltaTs = syncLogTimestamp - bizLogTimestamp;
				while (syncLogTimestamp != 0 && deltaTs < syncTsLatency) {
					try {
						Thread.sleep((syncTsLatency - deltaTs) * 1000L);
						LOG.info("business logs  is waitting  for flowing log seconds :"
								+ (syncTsLatency - deltaTs));
					} catch (InterruptedException e) {
						LOG.error("thread sleep interrupted", e);
					}
					syncLogTimestamp = syncTimestampMap.get(shardingKey)
							.intValue();
					deltaTs = syncLogTimestamp - bizLogTimestamp;
				}
			}

			// 更新为从本次扫描到的最后一条记录的rowkey，或当本次扫描未得到记录时则更新为本次setStopRow设置的rowkey
			byte[] bShard = { (byte) shardingKey };

			// add by yuanhong.shx 此处为了修复tt延迟，造成程序继续向下扫描的bug
			if (rowkey != null) {

				lastRowKey = Bytes.add(rowkey, pad);
				ttdelayflag = 0;
				bizOrderGenerator.delay = 0;

			} else {

				// 让延迟恢复后，第二次扫描可以顺利通过
				if (flagonce == 1) {

					ttdelayflag = 0;
					bizOrderGenerator.delay = 0;
					try {
						LOG.info("yanci get well , sleep 20 seconds for write data!!");
						Thread.sleep(20 * 1000L);
					} catch (InterruptedException e) {
						LOG.error("thread sleep interrupted", e);
					}

					return totalCount;
				}

				ttdelayflag++;
				bizOrderGenerator.delay = 1;

				try {
					Thread.sleep(5000L); // 停止5s
					LOG.info("stop scanning , no data  has been written recently! stop times : "
							+ ttdelayflag);
					if (lastRowKey != null) {
						int nowendtime = bizOrderGenerator.getLastTimestamp();

						int nowshard = lastRowKey[0];
						int nowstarttime = Bytes.toInt(lastRowKey, 1,
								Bytes.SIZEOF_INT);

						LOG.info("now try to scan the log : nowshard : "
								+ nowshard
								+ " nowstarttime : "
								+ nowstarttime
								+ " nowendtime : "
								+ (nowendtime % MbConstants.XXDF_HBASE_TIME_INTERVAL)
								+ " nowendtimestamp : " + nowendtime);

						// 当发生轮转时，向前滑动不再等待
						if (nowendtime % MbConstants.XXDF_HBASE_TIME_INTERVAL == 0) {

							ttdelayflag = 0;
							bizOrderGenerator.delay = 0;
							lastRowKey = Bytes
									.add(bShard,
											Bytes.toBytes(bizOrderGenerator
													.getLastTimestamp()
													% MbConstants.XXDF_HBASE_TIME_INTERVAL));
							LOG.info("rotate  happen , hash skipped  the 3 minutes!!");
							return totalCount;
						}

					}

				} catch (InterruptedException e) {
					LOG.error(
							"sleeping,   3 minitue agao  no data  has been written!",
							e);
				}

				// 凌晨3点到4点之间的延迟忽略,继续向前滑动
				int tthh = Integer.parseInt(TimeUtil.getTodayHour());
				if (tthh >= 3 && tthh <= 4) {

					ttdelayflag = 0;
					bizOrderGenerator.delay = 0;
					lastRowKey = Bytes.add(
							bShard,
							Bytes.toBytes(bizOrderGenerator.getLastTimestamp()
									% MbConstants.XXDF_HBASE_TIME_INTERVAL));

					return totalCount;
				}

				// 当日志源发生延迟后，等待15分钟，若数据还没到，则向前滑动4分钟。
				if (ttdelayflag > 200) {

					ttdelayflag = 0;
					bizOrderGenerator.delay = 0;
					lastRowKey = Bytes.add(
							bShard,
							Bytes.toBytes(bizOrderGenerator.getLastTimestamp()
									% MbConstants.XXDF_HBASE_TIME_INTERVAL));
					LOG.error("thread  has    skip   4 minutes   data !!!");
				}

			}

			/*
			 * lastRowKey = (rowkey != null) ? Bytes.add(rowkey, pad) :
			 * Bytes.add( bShard,
			 * Bytes.toBytes(bizOrderGenerator.getLastTimestamp() %
			 * Constants.XXDF_HBASE_TIME_INTERVAL));
			 */

			return totalCount;
		}

		/**
		 * 根据父订单的order id查找索引表得到各个子订单的order id
		 * 
		 * @param parentOrderId
		 * @return
		 */
		private List<byte[]> scanChildrenOrderId(String parentOrderId) {
			List<byte[]> childrenOrderKeyList = new ArrayList<byte[]>();
			ResultScanner rs = bizOrderIdxGenerator.scanIndexTable(shardingKey,
					Bytes.toBytes(parentOrderId));
			Result rr = new Result();
			if (rs != null) {
				while (rr != null) {
					try {
						rr = rs.next();
					} catch (IOException e) {
						LOG.error("failed rs.next() in scanChildrenOrderId()",
								e);
						return childrenOrderKeyList;
					}
					if (rr != null && !rr.isEmpty()) {
						byte[] value = rr.getValue(
								Bytes.toBytes(MbConstants.XXDF_HBASE_FAMILY),
								Bytes.toBytes(MbConstants.XXDF_HBASE_PB_COL));
						if (value != null) {
							childrenOrderKeyList.add(value);
						}
					}
				}
				rs.close();
			}
			return childrenOrderKeyList;
		}

		/**
		 * 根据子订单rowkey，批量查询获取子订单日志记录
		 * 
		 * @param childrenBizOrderKeyList
		 */
		private void getChildrenBizOrder(List<byte[]> childrenBizOrderKeyList) {
			Result[] bizOrderRes = bizOrderGenerator
					.getMainTable(childrenBizOrderKeyList);
			if (bizOrderRes != null && bizOrderRes.length > 0) {
				for (Result re : bizOrderRes) {
					if (re != null && !re.isEmpty()) {
						try {
							byte[] bizKey = re.getRow();
							byte[] bizValue = re.getValue(
									Bytes.toBytes(MbConstants.XXDF_HBASE_FAMILY),
									Bytes.toBytes(MbConstants.XXDF_HBASE_PB_COL));
							StarLogProtos.BusinessStarLog bizOrderLog = StarLogProtos.BusinessStarLog
									.parseFrom(bizValue);
							if (bizKey != null && bizOrderLog != null) {
								pendingChildBizOrderMap.put(
										new ByteArrayWrapper(bizKey),
										bizOrderLog);
							}
						} catch (Exception e) {
							LOG.error(
									"protobuf parsing error in getChildrenOrder()",
									e);
						}
					}
				}
			}
		}

		/**
		 * 查询pay_order表得到订单总钱数
		 * 
		 * @param bizOrder2ChildrenMap
		 * @return
		 */
		private Map<String, Long> getPayOrderTotalFee(
				TreeMap<String, Pair<List<byte[]>, String>> bizOrder2ChildrenMap) {
			// 根据biz_order表的父订单id，批量查询pay_order_index表
			Set<String> keySet = bizOrder2ChildrenMap.keySet();
			List<String> payOrderIdList = new ArrayList<String>(keySet);
			Map<String, Long> payOrder2TotalFeeMap = new HashMap<String, Long>();
			Result[] payOrderKeyRes = payOrderIdxGenerator
					.getIndexTable(payOrderIdList);
			List<byte[]> payOrderKeyList = new ArrayList<byte[]>();
			if (payOrderKeyRes != null && payOrderKeyRes.length > 0) {
				for (Result re : payOrderKeyRes) {
					if (re != null && !re.isEmpty()) {
						byte[] payKey = re.getRow();
						byte[] payValue = re.getValue(
								Bytes.toBytes(MbConstants.XXDF_HBASE_FAMILY),
								Bytes.toBytes(MbConstants.XXDF_HBASE_PB_COL));
						if (payKey != null && payValue != null)
							payOrderKeyList.add(payValue);
					}
				}
			}

			// 批量查询pay_order表得到父订单总钱数
			Result[] payOrderRes = null;
			if (payOrderKeyList.size() != 0)
				payOrderRes = payOrderGenerator.getMainTable(payOrderKeyList);
			if (payOrderRes != null && payOrderRes.length > 0) {
				for (Result re : payOrderRes) {
					if (re != null && !re.isEmpty()) {
						byte[] payKey = re.getRow();
						byte[] payValue = re.getValue(
								Bytes.toBytes(MbConstants.XXDF_HBASE_FAMILY),
								Bytes.toBytes(MbConstants.XXDF_HBASE_PB_COL));
						StarLogProtos.BusinessStarLog payOrderLog;
						try {
							payOrderLog = StarLogProtos.BusinessStarLog
									.parseFrom(payValue);
							if (payKey != null && payOrderLog != null) {
								payOrder2TotalFeeMap.put(
										payOrderLog.getOrderId(),
										payOrderLog.getActualTotalFee());
							}
						} catch (InvalidProtocolBufferException e) {
							LOG.error(
									"protobuf parsing error in getPayOrderTotalFee()",
									e);
						}
					}
				}
			}
			payOrderIdList.clear();
			payOrderIdList = null;
			payOrderKeyList.clear();
			payOrderKeyList = null;
			return payOrder2TotalFeeMap;

		}

		/**
		 * 根据父子订单关系，拆分子订单并通过queue发送给下级Bolt
		 * 
		 * @param bizOrder2ChildrenBizOrderMap
		 * @param payOrder2TotalFeeMap
		 */
		private void splitAndSendChildrenBizOrder(
				TreeMap<String, Pair<List<byte[]>, String>> bizOrder2ChildrenBizOrderMap,
				Map<String, Long> payOrder2TotalFeeMap) {
			// 遍历父订单到子订单的映射表
			for (Entry<String, Pair<List<byte[]>, String>> entry : bizOrder2ChildrenBizOrderMap
					.entrySet()) {
				// 取得父订单总钱数和计算拆分规则
				String orderId = entry.getKey();
				Pair<List<byte[]>, String> bizValue = entry.getValue();
				List<byte[]> bizVal = bizValue.getFirst();
				String payTime = bizValue.getSecond();
				long actualTotalFee = 0L;
				if (payOrder2TotalFeeMap.containsKey(orderId))
					actualTotalFee = payOrder2TotalFeeMap.get(orderId);
				long sum = 0L;
				int i = 0;
				for (byte[] bizChildOrder : bizVal) {
					ByteArrayWrapper wrapperKey = new ByteArrayWrapper(
							bizChildOrder);
					StarLogProtos.BusinessStarLog childOrder = pendingChildBizOrderMap
							.get(wrapperKey);
					if (childOrder != null) {
						long auctionPrice = childOrder.getAuctionPrice();
						long adjustFee = childOrder.getAdjustFee();
						long discountFee = childOrder.getDiscountFee();
						long buyAmount = childOrder.getBuyAmount();
						sum += (auctionPrice * buyAmount - discountFee + adjustFee);
						i++;
					}
				}
				if (i != bizVal.size()) { // TODO 父订单存在但子订单不齐的情况，暂时只输出错误日志！
					failPayOrderNum++;
					continue;
				}

				// 将父订单总钱数拆分给各子订单后向下级Bolt发送子订单
				for (byte[] bizChildOrder : bizVal) {
					ByteArrayWrapper wrapperKey = new ByteArrayWrapper(
							bizChildOrder);
					StarLogProtos.BusinessStarLog childOrder = pendingChildBizOrderMap
							.get(wrapperKey);
					if (childOrder != null) {
						long auctionPrice = childOrder.getAuctionPrice();
						long adjustFee = childOrder.getAdjustFee();
						long discountFee = childOrder.getDiscountFee();
						long buyAmount = childOrder.getBuyAmount();
						long actualFee = 0L;
						if (sum != 0L)
							actualFee = actualTotalFee
									* (auctionPrice * buyAmount - discountFee + adjustFee)
									/ sum;
						byte[] newValue = resetLog(childOrder, actualFee,
								payTime);
						try {
							long seller_id = 0;
							try {
								seller_id = Long.parseLong(childOrder
										.getSellerId());
							} catch (NumberFormatException e) {
								LOG.error("seller id format error", e);
							}
							long ts = childOrder.getOrderModifiedT();
							queue.put(new Values(seller_id, newValue, ts));
							emitPayOrderNum++;
						} catch (InterruptedException e) {
							LOG.error("failed to put data to queue", e);
							failPayOrderNum++;
						}
						pendingChildBizOrderMap.remove(wrapperKey);
					}
				}
			}
		}

		/**
		 * 清空日志记录中无用的字段
		 * 
		 * @param bizOrder
		 * @param actualTotalFee
		 * @param payTime
		 */
		private byte[] resetLog(BusinessStarLog bizOrder, long actualTotalFee,
				String payTime) {
			int payStatus = bizOrder.getPayStatus();
			/*
			 * if (payStatus != 1 && payStatus != 2) return new byte[0];
			 */
			Builder builder = StarLogProtos.BusinessStarLog.newBuilder();
			builder.setBuyerId(bizOrder.getBuyerId());
			builder.setAuctionId(bizOrder.getAuctionId());
			builder.setSellerId(bizOrder.getSellerId());
			builder.setShopId(bizOrder.getShopId()); // XXX: 目前日志中shopid均为0！！！
			builder.setBuyAmount(bizOrder.getBuyAmount());
			builder.setOrderId(bizOrder.getOrderId()); // 订单号
			builder.setPayStatus(bizOrder.getPayStatus());
			builder.setIsDetail(bizOrder.getIsDetail()); // 子订单
			builder.setIsNewGenerated(bizOrder.getIsNewGenerated()); // 拍下笔数
			builder.setOrderModifiedT(bizOrder.getOrderModifiedT()); // 日志生产时间戳
			builder.setGmtModified(bizOrder.getGmtModified());
			builder.setIsPay(bizOrder.getIsPay());
			
			
			builder.setIsMain(bizOrder.getIsMain()); // 子订单
			
			

			if (payStatus == 2) { // 成交订单
				builder.setIsPay(1);
				if (actualTotalFee == 0) {
					long auctionPrice = bizOrder.getAuctionPrice();
					long adjustFee = bizOrder.getAdjustFee();
					long discountFee = bizOrder.getDiscountFee();
					long buyAmount = bizOrder.getBuyAmount();
					actualTotalFee = (auctionPrice * buyAmount - discountFee + adjustFee);
					builder.setActualTotalFee(actualTotalFee);

				} else {
					builder.setActualTotalFee(actualTotalFee);
				}
				builder.setPayTime(payTime);
				// builder.setIsPay(1);
			}
			/*
			 * if (payStatus == 1) { // 拍下订单
			 * builder.setAuctionPrice(bizOrder.getAuctionPrice());
			 * builder.setGmtCreate(bizOrder.getGmtCreate());
			 * builder.setGmtModified(bizOrder.getGmtModified());
			 * builder.setIsPay(0); } else { // 成交订单
			 * builder.setActualTotalFee(actualTotalFee);
			 * builder.setPayTime(payTime); builder.setIsPay(1); }
			 */

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
				newRs = bizOrderGenerator
						.scanMainTable(shardingKey, lastRowKey);
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
		if (pc.getProperty(MbConstants.BUSINESS_SPOUT_QUEUE_SIZE) != null)
			queueSize = Integer.valueOf(pc
					.getProperty(MbConstants.BUSINESS_SPOUT_QUEUE_SIZE));
		this.queue = new LinkedBlockingQueue<Values>(queueSize);

		String startTs = pc
				.getProperty(MbConstants.BUSINESS_SPOUT_START_TIMESTAMP);
		if (startTs != null) { // 从指定时间开始扫描
			this.startTimestamp = Integer.valueOf(startTs);
		} else { // 按默认配置扫描
			this.startTimestamp = 0;
		}

		String endTs = pc.getProperty(MbConstants.BUSINESS_SPOUT_END_TIMESTAMP);
		if (endTs != null) { // 扫描到指定时间
			this.endTimestamp = Integer.valueOf(endTs);
		} else { // 按默认配置扫描
			this.endTimestamp = 0;
		}

		this.enableCompress = false;
		String enableCompressString = pc
				.getProperty(MbConstants.BUSINESS_SPOUT_ENABLE_COMPRESS);
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

		outputModePerShard = 5000;
		if (pc.getProperty(MbConstants.BUSINESS_SPOUT_OUTPUT_MODE_PER_SHARD) != null)
			outputModePerShard = Integer
					.valueOf(pc
							.getProperty(MbConstants.BUSINESS_SPOUT_OUTPUT_MODE_PER_SHARD));

		outputModePerSpout = 50000;
		if (pc.getProperty(MbConstants.BUSINESS_SPOUT_OUTPUT_MODE_PER_SPOUT) != null)
			outputModePerSpout = Integer
					.valueOf(pc
							.getProperty(MbConstants.BUSINESS_SPOUT_OUTPUT_MODE_PER_SPOUT));

		syncTsInterval = 1;
		if (pc.getProperty(MbConstants.BUSINESS_SPOUT_SYNC_TS_INTERVAL) != null)
			syncTsInterval = Integer.valueOf(pc
					.getProperty(MbConstants.BUSINESS_SPOUT_SYNC_TS_INTERVAL));

		syncTsLatency = 10;
		if (pc.getProperty(MbConstants.BUSINESS_SPOUT_SYNC_TS_LATENCY) != null)
			syncTsLatency = Integer.valueOf(pc
					.getProperty(MbConstants.BUSINESS_SPOUT_SYNC_TS_LATENCY));
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
					if ((MbConstants.ZK_CONF_PREFIX + MbConstants.ZK_SYNC_ACTIVE_BUSINESS_SPOUT)
							.equals(path) && data != null) {
						spoutActiveStatus = (data[0] != 0) ? true : false;
						LOG.info("zookeeper node change: active_business_spout="
								+ spoutActiveStatus);
					} else if ((MbConstants.ZK_CONF_PREFIX + MbConstants.ZK_SYNC_BUSINESS_SPOUT_SLEEP_RECORD)
							.equals(path) && data != null) {
						sleepRecord = Bytes.toLong(data);
						LOG.info("zookeeper node change: business_spout_sleep_record="
								+ sleepRecord);
					} else if ((MbConstants.ZK_CONF_PREFIX + MbConstants.ZK_SYNC_BUSINESS_SPOUT_SLEEP_TIME)
							.equals(path) && data != null) {
						sleepTime = Bytes.toLong(data);
						LOG.info("zookeeper node change: business_spout_sleep_time="
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
			LOG.error("failed to start business spout status zk listener", e);
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
					if (zkClient.checkExists().forPath(shardZkPath) == null) {
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
									zkRecoverTs - syncTsLatency));
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
								byte[] shardZkData = zkClient.getData()
										.forPath(shardZkPath);
								if (shardZkData == null)
									continue;
								int syncTs = Bytes.toInt(shardZkData);
								syncTimestampMap.put(shardingKey, syncTs);
							}
						} catch (Exception e) {
							LOG.error("failed to get data from zookeeper", e);
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
		this.discardOrderNum = 0L;
		this.emitGmtOrderNum = 0L;
		this.failGmtOrderNum = 0L;
		this.emitPayOrderNum = 0L;
		this.failPayOrderNum = 0L;
		this.emitBizOrderNum = 0L;
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
					SellerId2shopIdMap = (TLongLongHashMap) ois.readObject();
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
		if (SellerId2shopIdMap != null) {
			LOG.info("finish to load shop seller id, total count: "
					+ SellerId2shopIdMap.size());
			LOG.debug("shop id to seller id map: "
					+ SellerId2shopIdMap.toString());
		}
	}
	
	
	
	
	/**
	 * 清空日志记录中无用的字段
	 * 
	 * @param bizOrder
	 * @param actualTotalFee
	 * @param payTime
	 */
	public StarLogProtos.BusinessStarLog resetPreLog(BusinessStarLog bizOrder) {

		StarLogProtos.BusinessStarLog.Builder builder = StarLogProtos.BusinessStarLog
				.newBuilder(bizOrder);
		builder.setIsPay(1); // 是否付款

	    return builder.build();
	}
	
	
	
	
	

}
