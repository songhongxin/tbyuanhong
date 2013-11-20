package com.etao.lz.storm.detailcalc;

public class ShopItem {
	
	public long sellerId;    //用用户id
	public long alipayAmt;   //支付宝成交金额
	public long ts;   //时间戳
	public long px;   //1000万的倍数
	public boolean updateHistoryflag;
	
	public ShopItem(long sellerid) {
		this.sellerId = sellerid;
		this.alipayAmt = 0;
		this.ts = 0;
		this.px = 0;
		this.updateHistoryflag = false;
	}

}
