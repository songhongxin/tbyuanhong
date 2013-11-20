package com.etao.lz.storm.detailcalc;

import com.etao.lz.storm.tools.Constants;
import com.etao.lz.storm.utils.AdaptiveCounting;

/**
 * 指标对象
 * 
 * @author wxz
 * 
 */
public class UnitItem {

	public long sellerId; // 卖家ID
	public long auctionId; // 宝贝ID

	public long pv; //
	public long alipayAmt; // 支付宝成交金额
	public long alipayNum; // 支付宝成交件数
	public long tradeNum; // 拍下笔数
	public long alipayTradeNum; // 成交笔数
	public long alipayRate; // 当日支付率(atn / tn)
	public long uvValue; // 访客价值(amt / iuv)
	public long dealRate; // 成交转化率(auv / iuv)

	public AdaptiveCounting ab; // auv--成交uv位图信息
	public AdaptiveCounting ib; // auv--浏览uv信息

	public UnitItem(long sellerId, long auctionId) {
		this.sellerId = sellerId;
		this.auctionId = auctionId;

		this.pv = 0;
		this.alipayNum = 0;
		this.alipayAmt = 0;
		this.tradeNum = 0;
		this.alipayTradeNum = 0;
		this.alipayRate = 0;
		this.uvValue = 0;
		this.dealRate = 0;

		this.ab = new AdaptiveCounting(Constants.UV_BITMAP_K, true);
		this.ib = new AdaptiveCounting(Constants.UV_BITMAP_K, true);
	}

}
