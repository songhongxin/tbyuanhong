package com.etao.lz.storm.detailcalc;

import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.map.hash.TLongObjectHashMap;

import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.etao.lz.star.StarLogProtos;
import com.etao.lz.storm.tools.Constants;
import com.etao.lz.storm.utils.MysqlUtil;
import com.etao.lz.storm.utils.TimeUtils;

public class AlizhgDetail implements java.io.Serializable {

	/**
	 * 序列号
	 */
	private static final long serialVersionUID = 816467937922782894L;

	private static final Logger log = LoggerFactory
			.getLogger(AlizhgDetail.class);

	// 当前分钟成交金额指标计算结果存储
	private TLongObjectHashMap<ShopItem> curShopMap;

	// 当天成交金额指标累计计算结果存储
	private TLongObjectHashMap<ShopItem> dayShopMap;
	// 等待更新到mysql的分钟级别map队列
	public LinkedBlockingQueue<MysqlStoreItem> storeMysqlQueue;

	// 更新到mysql的频次，默认5分钟1次
	private final int mysqlFlushTimeInterval;

	// 存储当天时间戳的最小值，时间错比此小的都丢弃
	private long curMinDayTimestamp;
	// 存储当前mysql需要处理到的日志的时间戳的最大值
	private long curTimestamp;
	// 存储当天mysql相关需要处理到的日志的时间戳的最大值
	private long curMaxDayTimastamp;

	public AlizhgDetail() {

		this.curShopMap = new TLongObjectHashMap<ShopItem>();
		this.dayShopMap = new TLongObjectHashMap<ShopItem>();
		this.storeMysqlQueue = new LinkedBlockingQueue<MysqlStoreItem>(
				Constants.MYSQL_QUEUE_MAX);
		this.mysqlFlushTimeInterval = Constants.MYSQL_FLUSH_TT;

		this.curMinDayTimestamp = 0;
		this.curTimestamp = 0;
		this.curMaxDayTimastamp = 0;
	}

	// 获取当前计算结果
	public TLongObjectHashMap<ShopItem> getCurShopMap() {
		return curShopMap;
	}

	// 处理业务日志
	public void updateBusinessIndicators(
			StarLogProtos.BusinessStarLog LogEntry, long ts) {

		try {

			long sellerid = Long.parseLong(LogEntry.getSellerId());

			initTs(ts);

			if (ts < curMinDayTimestamp) {
				// 日志时间戳不是今天的，就丢弃
				return;
			}

			// 阿里指挥官实时指标需求start
			judgeTimeToUpdateMysql(ts);

			ShopItem shopItem;

			if (curShopMap.containsKey(sellerid)) {
				shopItem = curShopMap.get(sellerid);
			} else {
				shopItem = new ShopItem(sellerid);
				curShopMap.put(sellerid, shopItem);
			}
			StatOther.statUnitBiz(shopItem, ts, LogEntry);
			// 阿里指挥官实时指标需求end

		} catch (Exception e) {
			log.error("Failed to parse id because of exception. ", e);
		}

	}

	// 初始化各个时间标识
	private void initTs(long ts) {

		if (curTimestamp == 0) {
			curTimestamp = TimeUtils.getTimeCurMax(ts, mysqlFlushTimeInterval);
			curMaxDayTimastamp = TimeUtils.getTimeCurDayMax(ts);
			curMinDayTimestamp = TimeUtils.getTimeCurDayMin(ts);
		}

	}

	private void judgeTimeToUpdateMysql(long ts) {

		if (ts > curTimestamp || ts > curMaxDayTimastamp) {

			MysqlStoreItem storeItem = new MysqlStoreItem(curShopMap,
					curTimestamp);

			curShopMap = null;
			curShopMap = new TLongObjectHashMap<ShopItem>();

			if (ts > curMaxDayTimastamp) {
				storeItem.cleanFlagD = true;
				storeItem.ts = curMaxDayTimastamp;
				curMaxDayTimastamp = TimeUtils.getTimeCurDayMax(ts);
				curMinDayTimestamp = TimeUtils.getTimeCurDayMin(ts);
			}

			try {
				storeMysqlQueue.add(storeItem);
			} catch (Exception e) {
				log.error("Failed to add this item to mysql queue, it may be full now.");
				e.printStackTrace();
			}

			curTimestamp = TimeUtils.getTimeCurMax(ts, mysqlFlushTimeInterval);
		}

	}

	// 更新店铺级别指标
	private TLongObjectHashMap<ShopItem> updateShopMap(
			TLongObjectHashMap<ShopItem> cur_map, long ts) {

		for (TLongObjectIterator<ShopItem> itr = cur_map.iterator(); itr
				.hasNext();) {
			itr.advance();
			long sellerid = itr.key();
			ShopItem val = itr.value();

			if (dayShopMap.size() <= 0) {
				MysqlUtil.getShopDayMap(dayShopMap, ts);
			}
			ShopItem ex_item = dayShopMap.get(sellerid);
			if (ex_item == null) {
				ex_item = new ShopItem(sellerid);
				dayShopMap.put(sellerid, ex_item);
			}

			val.alipayAmt += ex_item.alipayAmt;
			ex_item.alipayAmt = val.alipayAmt;

			long cur_px = ex_item.alipayAmt / Constants.ALIPAYAMT_EVENT_BASE;
			if (cur_px > ex_item.px) {
				ex_item.px = cur_px;
				val.updateHistoryflag = true;
				if (val.ts == 0) {
					val.ts = ts;
				}
			}
		}

		return cur_map;

	}

	public TLongObjectHashMap<ShopItem> getToMysqlList(MysqlStoreItem storeItem) {

		TLongObjectHashMap<ShopItem> storeMap = storeItem.storeMap;

		log.info("The number of list to update to mysql is " + storeMap.size());

		long ts = storeItem.ts;

		storeMap = updateShopMap(storeMap, ts);

		if (storeItem.cleanFlagD) {
			dayShopMap = null;
			dayShopMap = new TLongObjectHashMap<ShopItem>();
		}

		return storeMap;

	}
}