package com.etao.lz.storm.detailcalc;

//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

import com.etao.lz.star.StarLogProtos.BusinessStarLog;
import com.etao.lz.star.StarLogProtos.FlowStarLog;

public class Stat {

//	private static final Logger log = LoggerFactory.getLogger(Stat.class);
	
	//计算宝贝业务指标
	public static void statUnitBiz(UnitItem item, BusinessStarLog LogEntry) {
		
		int detail = LogEntry.getIsDetail();
		int isnew = LogEntry.getIsNewGenerated();
		int ispay = LogEntry.getIsPay();
		
		if(detail == 1 && ispay == 1) {
			item.alipayTradeNum++; //支付宝成交笔数
			item.alipayNum += LogEntry.getBuyAmount(); //支付宝成交件数
			item.alipayAmt += LogEntry.getActualTotalFee(); //支付宝成交金额
			item.ab.offer(LogEntry.getBuyerId());
		}
		if(detail == 1 && isnew == 1) {
			item.tradeNum++; //拍下笔数
		}
	}
	
	//宝贝流量指标
	public static void statUnitFlow(UnitItem item, FlowStarLog LogEntry) {
		
		item.pv++;
		item.ib.offer(LogEntry.getUidMid());
		
	}
}