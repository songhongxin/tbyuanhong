package com.etao.lz.storm.detailcalc;

import com.etao.lz.storm.tools.Constants;
import com.etao.lz.storm.utils.AdaptiveCounting;

public class UserAccessEntry {
	
	public long sellerId;   //店铺标识
	
	public long pv;   //
	public long alipayAmt;   //支付宝成交金额
	public long alipayNum;   //支付宝成交件数 
	public long tradeNum;    //拍下笔数
	public long alipayTradeNum;    //成交笔数
	public AdaptiveCounting ab;       //auv--成交uv位图信息
	public AdaptiveCounting ib;       //auv--浏览uv信息
	
	public UserAccessEntry(long sellerid) {
		sellerId = sellerid;
		pv = 0;
		alipayAmt = 0;
		alipayNum = 0;
		tradeNum = 0;
		alipayTradeNum = 0;
		ab  = new AdaptiveCounting(Constants.UV_BITMAP_K,true);     //采用稀疏位图的结构
		ib  = new AdaptiveCounting(Constants.UV_BITMAP_K,true);
	}
	
}