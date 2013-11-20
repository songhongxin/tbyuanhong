package com.etao.lz.storm.detailcalc;

import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;

import com.etao.lz.star.StarLogProtos.BusinessStarLog;
import com.etao.lz.star.StarLogProtos.FlowStarLog;
import com.etao.lz.storm.utils.IndicatorUtil;

@Slf4j
public class JhcDeltaDetailCalc implements DeltaDetailCalc {

	TIntObjectHashMap<TLongObjectHashMap<TLongObjectHashMap<UnitItem>>> hourlyDeltas = new TIntObjectHashMap<TLongObjectHashMap<TLongObjectHashMap<UnitItem>>>();
	TIntObjectHashMap<TLongObjectHashMap<TLongObjectHashMap<UnitItem>>> dailyDeltas = new TIntObjectHashMap<TLongObjectHashMap<TLongObjectHashMap<UnitItem>>>();

	// 统计各级别内存结构的元素数量
	int[] hourlyDeltaStats = new int[3];
	int[] dailyDeltaStats = new int[3];

	@Override
	public void updateTrafficIndicators(FlowStarLog logEntry) {
		try {
			long msEpoch = logEntry.getTs();
			int hourlyTs = IndicatorUtil.epochMsToBcd(msEpoch);
			int dailyTs = IndicatorUtil.bcdToDateEpoch(hourlyTs);
			long sellerId = Long.parseLong(logEntry.getShopid());
			long auctionId = Long.parseLong(logEntry.getAuctionid());

			// 累计小时级宝贝指标增量
			UnitItem hourlyDelta = needUpdateHourlyItem(hourlyTs, sellerId,
					auctionId);
			hourlyDelta.pv++;
			hourlyDelta.ib.offer(logEntry.getUidMid());

			// 累计当日宝贝指标增量
			UnitItem dailyDelta = needUpdateDailyItem(dailyTs, sellerId,
					auctionId);
			dailyDelta.pv++;
			dailyDelta.ib.offer(logEntry.getUidMid());
		} catch (Exception e) {
			log.error("Failed to process traffic log", e);
		}
	}

	@Override
	public void updateBusinessIndicators(BusinessStarLog logEntry) {
		try {
			long msEpoch = logEntry.getOrderModifiedT();
			int hourlyTs = IndicatorUtil.epochMsToBcd(msEpoch);
			int dailyTs = IndicatorUtil.bcdToDateEpoch(hourlyTs);
			long sellerId = Long.parseLong(logEntry.getSellerId());
			long auctionId = Long.parseLong(logEntry.getAuctionId());
			int isDetail = logEntry.getIsDetail();
			int isNew = logEntry.getIsNewGenerated();
			int isPay = logEntry.getIsPay();

			UnitItem hourlyDelta = needUpdateHourlyItem(hourlyTs, sellerId,
					auctionId);
			UnitItem dailyDelta = needUpdateDailyItem(dailyTs, sellerId,
					auctionId);

			if (isDetail == 1 && isPay == 1) {
				// 累计小时级宝贝指标增量
				hourlyDelta.alipayTradeNum++; // 支付宝成交笔数
				hourlyDelta.alipayNum += logEntry.getBuyAmount(); // 支付宝成交件数
				hourlyDelta.alipayAmt += logEntry.getActualTotalFee(); // 支付宝成交金额
				hourlyDelta.ab.offer(logEntry.getBuyerId());

				// 累计当日宝贝指标增量
				dailyDelta.alipayTradeNum++; // 支付宝成交笔数
				dailyDelta.alipayNum += logEntry.getBuyAmount(); // 支付宝成交件数
				dailyDelta.alipayAmt += logEntry.getActualTotalFee(); // 支付宝成交金额
				dailyDelta.ab.offer(logEntry.getBuyerId());
			}

			if (isDetail == 1 && isNew == 1) {
				// 累计小时级宝贝指标增量
				hourlyDelta.tradeNum++; // 拍下笔数
				// 累计当日宝贝指标增量
				dailyDelta.tradeNum++; // 拍下笔数
			}
		} catch (Exception e) {
			log.error("Failed to process biz log", e);
		}
	}

	@Override
	public TIntObjectHashMap<TLongObjectHashMap<TLongObjectHashMap<UnitItem>>> flushHourlyItems() {
		TIntObjectHashMap<TLongObjectHashMap<TLongObjectHashMap<UnitItem>>> res = hourlyDeltas;
		hourlyDeltas = new TIntObjectHashMap<TLongObjectHashMap<TLongObjectHashMap<UnitItem>>>();
		hourlyDeltaStats = new int[3];
		return res;
	}

	@Override
	public TIntObjectHashMap<TLongObjectHashMap<TLongObjectHashMap<UnitItem>>> flushDailyItems() {
		TIntObjectHashMap<TLongObjectHashMap<TLongObjectHashMap<UnitItem>>> res = dailyDeltas;
		dailyDeltas = new TIntObjectHashMap<TLongObjectHashMap<TLongObjectHashMap<UnitItem>>>();
		dailyDeltaStats = new int[3];
		return res;
	}

	UnitItem needUpdateHourlyItem(int hourlyTs, long sellerId, long auctionId) {
		TLongObjectHashMap<TLongObjectHashMap<UnitItem>> t1 = hourlyDeltas
				.get(hourlyTs);
		if (t1 == null) {
			t1 = new TLongObjectHashMap<TLongObjectHashMap<UnitItem>>();
			hourlyDeltas.put(hourlyTs, t1);
			hourlyDeltaStats[0]++;
		}
		TLongObjectHashMap<UnitItem> t2 = t1.get(sellerId);
		if (t2 == null) {
			t2 = new TLongObjectHashMap<UnitItem>();
			t1.put(sellerId, t2);
			hourlyDeltaStats[1]++;
		}
		UnitItem res = t2.get(auctionId);
		if (res == null) {
			res = new UnitItem(sellerId, auctionId);
			t2.put(auctionId, res);
			hourlyDeltaStats[2]++;
		}
		return res;
	}

	UnitItem needUpdateDailyItem(int dailyTs, long sellerId, long auctionId) {
		TLongObjectHashMap<TLongObjectHashMap<UnitItem>> t1 = dailyDeltas
				.get(dailyTs);
		if (t1 == null) {
			t1 = new TLongObjectHashMap<TLongObjectHashMap<UnitItem>>();
			dailyDeltas.put(dailyTs, t1);
			dailyDeltaStats[0]++;
		}
		TLongObjectHashMap<UnitItem> t2 = t1.get(sellerId);
		if (t2 == null) {
			t2 = new TLongObjectHashMap<UnitItem>();
			t1.put(sellerId, t2);
			dailyDeltaStats[1]++;
		}
		UnitItem res = t2.get(auctionId);
		if (res == null) {
			res = new UnitItem(sellerId, auctionId);
			t2.put(auctionId, res);
			dailyDeltaStats[2]++;
		}
		return res;
	}

	@Override
	public void dumpInfo() {
		log.info(String.format(
				"Total items in hourly deltas: %d ts - %d seller - %d auction",
				hourlyDeltaStats[0], hourlyDeltaStats[1], hourlyDeltaStats[2]));
		int[] hourlyTs = hourlyDeltas.keys();
		log.info(String.format("Current hourly timestamps: %s",
				StringUtils.join(ArrayUtils.toObject(hourlyTs), ",")));
		log.info(String.format(
				"Total items in daily deltas: %d ts - %d seller - %d auction",
				dailyDeltaStats[0], dailyDeltaStats[1], dailyDeltaStats[2]));
		int[] dailyTs = dailyDeltas.keys();
		log.info(String.format("Current daily timestamps: %s",
				StringUtils.join(ArrayUtils.toObject(dailyTs), ",")));
	}

}
