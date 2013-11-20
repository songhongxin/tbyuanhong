package com.etao.lz.storm;

import gnu.trove.map.hash.TLongObjectHashMap;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import com.etao.lz.star.StarLogProtos;
import com.etao.lz.storm.detailcalc.AlizhgDetail;
import com.etao.lz.storm.detailcalc.JhcDetail;
import com.etao.lz.storm.detailcalc.MysqlStoreItem;
import com.etao.lz.storm.detailcalc.ShopItem;
import com.etao.lz.storm.detailcalc.StoreItem;
import com.etao.lz.storm.detailcalc.UnitItem;
import com.etao.lz.storm.tools.Constants;
import com.etao.lz.storm.utils.MysqlUtil;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * 
 * 根据流量日志和业务日志进行处理： 1、得到宝贝级别流量和业务指标结果，1分钟1次更新到hbase表中
 * 2、得到店铺级别成交金额结果，5分钟1次更新到mysql表中
 * 
 * @author muqian.chr
 * 
 */
public class DetailBolt extends BaseRichBolt {

	private static final long serialVersionUID = -5660298004139920055L;

	private static final Logger log = LoggerFactory.getLogger(DetailBolt.class);

	/** Storm 任务上下文 */
	private int taskId;
	private String componentId;
	private transient OutputCollector collector;
	private transient TTSpoutMonitor _monitor;

	private JhcDetail jhcDetail;
	private AlizhgDetail zhgDetail;

	public DetailBolt() {
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

		jhcDetail = new JhcDetail(true);
	//	zhgDetail = new AlizhgDetail();

		// 开启更新hbase进程
		flushMapToHbase(jhcDetail.storeHbseQueue);
		// 开启更新mysql进程
	//	flushMapToMysql(zhgDetail.storeMysqlQueue);
	}

	@Override
	public void execute(Tuple input) {
		try {
			String streamID = input.getSourceStreamId();
			byte[] logBytes = input.getBinaryByField("log");

			if (logBytes != null && logBytes.length > 1) {
				long ts = input.getLongByField("ts") / 1000;
				if (Constants.TRAFFIC_STREAMID.equals(streamID)) {
					// 流量日志处理
					StarLogProtos.FlowStarLog LogEntry = StarLogProtos.FlowStarLog
							.parseFrom(logBytes);

					// 调试信息start
					/*
					try {
						String sellerid = LogEntry.getShopid();
						if (sellerid.equals("305358018")) {
							debugFlowLog(LogEntry);
						}
					} catch (Exception e) {
						log.error("Failed to parse sellerid. ", e);
					}
					*/
					// 调试信息end

					// 根据日志计算指标
					jhcDetail.updateTrafficIndicators(LogEntry, ts);

				} else if (Constants.BIZ_STREAMID.equals(streamID)) {
					// 业务日志处理
					StarLogProtos.BusinessStarLog LogEntry = StarLogProtos.BusinessStarLog
							.parseFrom(logBytes);

					// 调试信息start
					try {
						String sellerid = LogEntry.getSellerId();
						if (sellerid.equals("305358018")) {
							debugBizLog(LogEntry);
						}
					} catch (Exception e) {
						log.error("Failed to parse sellerid. ", e);
					}
					// 调试信息end

					// 根据日志计算指标
					jhcDetail.updateBusinessIndicators(LogEntry, ts);
				//	zhgDetail.updateBusinessIndicators(LogEntry, ts);
				}
				_monitor.sign(ts * 1000);
			}
		} catch (InvalidProtocolBufferException e) {
			log.error("Failed to parse log", e);
		} catch (Exception e) {
			log.error("Error occured", e);
		}
		collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	// 队列不为空，就更新其内容到hbase
	private void flushMapToHbase(
			final LinkedBlockingQueue<StoreItem> storeHbseQueue) {
		Thread thr = new Thread(componentId + ":" + taskId + "-"
				+ "flushToHbase") {
			@Override
			public void run() {
				while (!Thread.interrupted()) {
					try {
						Thread.sleep(500);
					} catch (InterruptedException e) {
						log.error("Thread sleep interrupted", e);
					}

					StoreItem storeItem;
					while ((storeItem = storeHbseQueue.poll()) != null) {

						// StoreItem storeItem = storeHbseQueue.poll();

						ArrayList<UnitItem> flushHourList = new ArrayList<UnitItem>();
						ArrayList<UnitItem> flushDayList = new ArrayList<UnitItem>();

						jhcDetail.getToHbaseList(storeItem, flushDayList,
								flushHourList);

						long ts = storeItem.ts;

						try {
							log.info("Start to put list to hbase.");
							jhcDetail.hbaseH.putUnit(flushHourList, ts, "h");
							jhcDetail.hbaseD.putUnit(flushDayList, ts, "d");
							log.info("End to put list to hbase.");
						} catch (Exception e) {
							log.error("Error when put list to hbase", e);
						}

					}
				}
			}
		};
		thr.setDaemon(true);
		thr.start();
	}

	// 队列不为空，就更新其内容到mysql
	@SuppressWarnings("unused")
	private void flushMapToMysql(
			final LinkedBlockingQueue<MysqlStoreItem> storeMysqlQueue) {
		Thread thr = new Thread(componentId + ":" + taskId + "-"
				+ "flushToMysql") {
			@Override
			public void run() {
				while (!Thread.interrupted()) {
					try {
						Thread.sleep(5000);
					} catch (InterruptedException e) {
						log.error("Thread sleep interrupted", e);
					}
					MysqlStoreItem storeItem;
					while ((storeItem = storeMysqlQueue.poll()) != null) {

						log.info("The number of mysql storequeue is "
								+ storeMysqlQueue.size());

						// MysqlStoreItem storeItem = storeMysqlQueue.poll();

						TLongObjectHashMap<ShopItem> storeMap = zhgDetail
								.getToMysqlList(storeItem);

						long ts = storeItem.ts;

						try {
							log.info("Start to put list to mysql.");
							MysqlUtil.writeShopDayRes(storeMap, ts);
							log.info("End to put list to mysql.");
						} catch (Exception e) {
							log.error("Error when put unit list to mysql.", e);
						}

					}
				}
			}
		};
		thr.setDaemon(true);
		thr.start();
	}

	private final class TTSpoutMonitor extends AppMonitor {

		private static final long serialVersionUID = 2694180049179434218L;

		@SuppressWarnings("rawtypes")
		public TTSpoutMonitor(String name, Map conf, TopologyContext context) {
			super(name, conf, context);
			// TODO Auto-generated constructor stub
		}

		@Override
		protected int getQueueSize() {
			return jhcDetail.storeHbseQueue.size();
		}

	}

	private void debugBizLog(StarLogProtos.BusinessStarLog business) {

		StringBuffer st = new StringBuffer();

		String sellerid = business.getSellerId();
		String auctionid = business.getAuctionId();

		String money = String.valueOf(business.getActualTotalFee());

		int isdetail = business.getIsDetail();

		int pay = business.getIsPay();

		String order_id = business.getOrderId();

		String time;

		if (pay == 1) {
			time = business.getPayTime();

		} else {
			time = business.getGmtModified();
		}

		st.append(" debug businesslog by muqian.chr time: ");
		st.append(time);
		st.append(" orded_id:");
		st.append(order_id);
		st.append(" isdetail:");
		st.append(isdetail);
		st.append(" money:");
		st.append(money);
		st.append(" sellereid:");
		st.append(sellerid);
		st.append(" auctionid:");
		st.append(auctionid);
		st.append(" ispay:");
		st.append(pay);

		log.info(st.toString());

	}
	
	@SuppressWarnings("unused")
	private void debugFlowLog(StarLogProtos.FlowStarLog business) {

		StringBuffer st = new StringBuffer();

		String sellerid = business.getShopid();
		String auctionid = business.getAuctionid();
		long time = business.getTs();

		st.append(" debug flowlog by muqian.chr 时间 ：");
		st.append(time);
		st.append(" sellereid  : ");
		st.append(sellerid);
		st.append(" auctionid: ");
		st.append(auctionid);

		log.info(st.toString());

	}

}