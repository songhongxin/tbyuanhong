package com.etao.lz.storm.detailcalc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.etao.lz.star.StarLogProtos.BusinessStarLog;

public class StatOther {


	private static final Logger log = LoggerFactory.getLogger(StatOther.class);
	//计算店铺整体成交情况
	public static void statUnitBiz(ShopItem shopItem, long ts, BusinessStarLog LogEntry) {
		
	//	int detail = LogEntry.getIsDetail();
		int ispay = LogEntry.getIsPay();
		int  main = LogEntry.getIsMain();
		
		if(main == 1 && ispay == 1) {
			long fee = LogEntry.getActualTotalFee();
			shopItem.alipayAmt += fee;
			shopItem.ts = ts;
			if(fee == 0) {
				log.info("debug yuanhong mysql alipay_amt : sellerid:" + shopItem.sellerId + " auctionid:" + LogEntry.getAuctionId());
			}
		}
	}
	
}