package com.etao.lz.storm.detailcalc;

import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.map.hash.TLongObjectHashMap;

import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.etao.lz.hbase.HbaseManager;
import com.etao.lz.star.StarLogProtos;
import com.etao.lz.storm.tools.Constants;
import com.etao.lz.storm.utils.AdaptiveCounting;
import com.etao.lz.storm.utils.TimeUtils;

public class JhcDetail implements java.io.Serializable {

	/**
	 * 序列号
	 */
	private static final long serialVersionUID = -9022593246015401203L;

	private static final Logger log = LoggerFactory.getLogger(JhcDetail.class);

	// 当前分钟流量、业务日志指标计算结果存储
	private TLongObjectHashMap<UnitItem> curItemMap;

	// 当天流量、业务日志指标累计结果存储
	private TLongObjectHashMap<UserAccessEntry> allItemMap;
	// 当前小时天流量、业务日志指标累计结果存储
	private TLongObjectHashMap<UserAccessEntry> hourItemMap;

	// 等待更新到hbase的分钟级别map的队列
	public LinkedBlockingQueue<StoreItem> storeHbseQueue;

	// 存储当前hbase相关需要处理到的日志的时间戳的最大值
	private long curTimestamp;
	// 存储当前小时hbase相关需要处理到的日志的时间戳的最大值
	private long curMaxHourTimestamp;
	// 存储当天hbase相关需要处理到的日志的时间戳的最大值
	private long curMaxDayTimestamp;
	// 存储当天时间戳的最小值，时间错比此小的都丢弃
	private long curMinDayTimestamp;

	private int hbaseFlushTimeInterval;

	public HbaseManager hbaseD;
	public HbaseManager hbaseH;

	public boolean htest = true; // 是否开启初始化hbase的

	public JhcDetail(boolean htest) {

		this.curItemMap = new TLongObjectHashMap<UnitItem>();
		this.allItemMap = new TLongObjectHashMap<UserAccessEntry>();
		this.hourItemMap = new TLongObjectHashMap<UserAccessEntry>();
		this.storeHbseQueue = new LinkedBlockingQueue<StoreItem>(
				Constants.HBASE_QUEUE_MAX);

		this.hbaseFlushTimeInterval = Constants.HBASE_FLUSH_TT;
		this.htest = htest;

		if (htest == true) {

			this.hbaseD = new HbaseManager(Constants.DP_D_TABLE);
			this.hbaseH = new HbaseManager(Constants.DP_H_TABLE);
		}
		this.curTimestamp = 0;
		this.curMaxHourTimestamp = 0;
		this.curMaxDayTimestamp = 0;
		this.curMinDayTimestamp = 0;

	}

	// 获取当前分钟的状态
	public TLongObjectHashMap<UnitItem> getCurItemMap() {
		return curItemMap;
	}

	private void initTs(long ts) {

		if (curTimestamp == 0) {
			curTimestamp = TimeUtils.getTimeCurMax(ts, hbaseFlushTimeInterval);
			curMaxHourTimestamp = TimeUtils.getTimeCurHourMax(ts);
			curMaxDayTimestamp = TimeUtils.getTimeCurDayMax(ts);
			curMinDayTimestamp = TimeUtils.getTimeCurDayMin(ts);
		}

	}

	private void judgeTimeToUpdateHbase(long ts) {

		if (ts > curTimestamp || ts > curMaxHourTimestamp) {
			// 日志处理到下一时间段，需要将当前时间段的处理结果放到更新队列
			StoreItem storeItem = new StoreItem(curItemMap, curTimestamp);
			curItemMap = null;
			curItemMap = new TLongObjectHashMap<UnitItem>();

			if (ts > curMaxHourTimestamp) {
				// 日志处理到下一小时，需要将小时指标结果重置
				storeItem.cleanFlagH = true;
				storeItem.ts = curMaxHourTimestamp;
				curMaxHourTimestamp = TimeUtils.getTimeCurHourMax(ts);

				if (ts > curMaxDayTimestamp) {
					// 日志处理到下一天，需要将当天指标清楚
					storeItem.cleanFlagD = true;
					curMaxDayTimestamp = TimeUtils.getTimeCurDayMax(ts);
					curMinDayTimestamp = TimeUtils.getTimeCurDayMin(ts);
				}
			}
			storeHbseQueue.add(storeItem);

			if (storeHbseQueue.size() > Constants.STOREMAP_MAX_SIZE) {
				// 当队列长度过长时，调整更新频度
				hbaseFlushTimeInterval = Constants.HBASE_FLUSH_NEW_TT;
			} else if (storeHbseQueue.size() < Constants.STOREMAP_MIN_SIZE) {
				hbaseFlushTimeInterval = Constants.HBASE_FLUSH_TT;
			}

			log.info("the number of storequeue " + storeHbseQueue.size());

			curTimestamp = TimeUtils.getTimeCurMax(ts, hbaseFlushTimeInterval);
		}

	}

	// 处理流量日志
	public void updateTrafficIndicators(StarLogProtos.FlowStarLog LogEntry,
			long ts) {

		try {

			long sellerid = Long.parseLong(LogEntry.getShopid());
			long auctionid = Long.parseLong(LogEntry.getAuctionid());

			initTs(ts);

			judgeTimeToUpdateHbase(ts);

			UnitItem item = curItemMap.get(auctionid);

			if (item == null) {
				item = new UnitItem(sellerid, auctionid);
				curItemMap.put(auctionid, item);
			}

			Stat.statUnitFlow(item, LogEntry);

		} catch (Exception e) {
			log.error("Failed to parse sellerid/auctionid. ", e);
		}
	}

	// 处理业务日志
	public void updateBusinessIndicators(
			StarLogProtos.BusinessStarLog LogEntry, long ts) {

		try {

			long sellerid = Long.parseLong(LogEntry.getSellerId());
			long auctionid = Long.parseLong(LogEntry.getAuctionId());

			initTs(ts);

			if (ts < curMinDayTimestamp) {
				// 日志时间戳不是今天的，就丢弃
				return;
			}

			// 江湖策实时指标需求start
			judgeTimeToUpdateHbase(ts);

			UnitItem item = curItemMap.get(auctionid);

			if (item == null) {
				item = new UnitItem(sellerid, auctionid);
				curItemMap.put(auctionid, item);
			}
			Stat.statUnitBiz(item, LogEntry);
			// 江湖策实时指标需求end

		} catch (Exception e) {
			log.error("Failed to parse sellerid/auctionid. ", e);
		}

	}

	// 更新宝贝级别指标
	public void updateItemMap(TLongObjectHashMap<UnitItem> curMap, long ts,
			ArrayList<UnitItem> flushDayList, ArrayList<UnitItem> flushHourList) {

		for (TLongObjectIterator<UnitItem> itr = curMap.iterator(); itr
				.hasNext();) {
			long auctionid = 0;
			UnitItem val = null;
			itr.advance();

			auctionid = itr.key();
			val = itr.value();
			long sellerid = val.sellerId;

			UserAccessEntry exItem = this.allItemMap.get(auctionid);
			UserAccessEntry exHourItem = this.hourItemMap.get(auctionid);
			if (exItem == null) {
				exItem = new UserAccessEntry(sellerid);
				if (htest == true) {
			//		hbaseD.readItemDay(exItem, sellerid, auctionid, ts);
				}
				this.allItemMap.put(auctionid, exItem);
			}
			if (exHourItem == null) {
				exHourItem = new UserAccessEntry(sellerid);
				if (htest == true) {
			//		 hbaseH.readItemHour(exHourItem, sellerid, auctionid, ts);
				}
				this.hourItemMap.put(auctionid, exHourItem);
			}

			UnitItem valDay = getUnitResult(sellerid, auctionid, exItem, val);
			UnitItem valHour = getUnitResult(sellerid, auctionid, exHourItem,
					val);

			flushHourList.add(valHour);
			flushDayList.add(valDay);
		}

	}

	public UnitItem getUnitResult(long sellerid, long auctionid,
			UserAccessEntry exItem, UnitItem val) {

		UnitItem valR = new UnitItem(sellerid, auctionid);

		valR.pv = exItem.pv += val.pv;
		valR.alipayAmt = exItem.alipayAmt += val.alipayAmt;
		valR.alipayNum = exItem.alipayNum += val.alipayNum;
		valR.alipayTradeNum = exItem.alipayTradeNum += val.alipayTradeNum;
		valR.tradeNum = exItem.tradeNum += val.tradeNum;
		if (valR.tradeNum > 0) {
			valR.alipayRate = valR.alipayTradeNum * 10000 / valR.tradeNum;
		}

		long auv = 0, iuv = 0;

		try {
			valR.ab = (AdaptiveCounting) exItem.ab.merge(val.ab);
			exItem.ab = valR.ab;
			auv = valR.ab.cardinality();
		} catch (Exception e) {
			log.error("Failed to merge ab bitmap. ", e);
		}

		try {
			valR.ib = (AdaptiveCounting) exItem.ib.merge(val.ib);
			exItem.ib = valR.ib;
			iuv = valR.ib.cardinality();

			if (iuv > 0) {
				valR.uvValue = valR.alipayAmt * 100 / iuv;
				valR.dealRate = auv * 10000 / iuv;
			}
		} catch (Exception e) {
			log.error("Failed to merge ib bitmap. ", e);
		}

		return valR;

	}

	public void getToHbaseList(StoreItem storeItem,
			ArrayList<UnitItem> flushDayList, ArrayList<UnitItem> flushHourList) {

		TLongObjectHashMap<UnitItem> storeMap = storeItem.storeMap;

		log.info("The number of map this time is " + storeMap.size());

		long ts = storeItem.ts;

		// 将storemap与累计值更新
		updateItemMap(storeMap, ts, flushDayList, flushHourList);

		if (storeItem.cleanFlagD) {
			allItemMap = null;
			allItemMap = new TLongObjectHashMap<UserAccessEntry>();
		} else if (storeItem.cleanFlagH) {
			hourItemMap = null;
			hourItemMap = new TLongObjectHashMap<UserAccessEntry>();
		}

		storeMap = null;
	}

}