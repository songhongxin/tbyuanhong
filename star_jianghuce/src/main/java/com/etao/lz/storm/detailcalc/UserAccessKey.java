package com.etao.lz.storm.detailcalc;

/*
 * 
 * 暂时不采用该数据结构了
 * 
 * */

public class UserAccessKey implements Comparable<Object> {

	public long sellerId;    //用户id
	public long auctionId;   //宝贝id
	
	public UserAccessKey(long sellerid, long auction_id) {
		this.sellerId = sellerid;
		this.auctionId = auction_id;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (auctionId ^ (auctionId >>> 32));
		result = prime * result + (int) (sellerId ^ (sellerId >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		UserAccessKey other = (UserAccessKey) obj;
		if (auctionId != other.auctionId)
			return false;
		if (sellerId != other.sellerId)
			return false;
		return true;
	}

	@Override
	public int compareTo(Object o) {
		
		if(this.sellerId > ((UserAccessKey) o).sellerId) {
			return 1; 
		} else if(this.sellerId < ((UserAccessKey) o).sellerId) {
			return -1; 
		} else if(this.auctionId > ((UserAccessKey) o).auctionId) {
			return 1; 
		} else if(this.auctionId < ((UserAccessKey) o).auctionId) {
			return -1; 
		} else {
			return 0;
		}
		
	}

}