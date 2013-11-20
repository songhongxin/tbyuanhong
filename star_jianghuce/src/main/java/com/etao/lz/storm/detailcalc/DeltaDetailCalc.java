package com.etao.lz.storm.detailcalc;

import gnu.trove.map.hash.TIntObjectHashMap;
import gnu.trove.map.hash.TLongObjectHashMap;

import com.etao.lz.star.StarLogProtos;

/**
 * 分时段计算指标 delta 值接口
 * 
 * @author wxz
 * 
 */
public interface DeltaDetailCalc {

	void updateTrafficIndicators(StarLogProtos.FlowStarLog logEntry);

	void updateBusinessIndicators(StarLogProtos.BusinessStarLog logEntry);

	/**
	 * hourly ts -> {seller_id -> {auction_id -> hourly unit_item deltas}}
	 */
	TIntObjectHashMap<TLongObjectHashMap<TLongObjectHashMap<UnitItem>>> flushHourlyItems();

	/** daily ts -> {seller_id -> {auction_id -> daily unit_item deltas}} */
	TIntObjectHashMap<TLongObjectHashMap<TLongObjectHashMap<UnitItem>>> flushDailyItems();

	void dumpInfo();
}
