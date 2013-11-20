package com.etao.lz.storm.detailcalc;

import gnu.trove.map.hash.TLongObjectHashMap;

public class MysqlStoreItem {

	public long ts;
	public TLongObjectHashMap<ShopItem> storeMap;
	// public boolean cleanFlagH;
	public boolean cleanFlagD;

	public MysqlStoreItem(TLongObjectHashMap<ShopItem> store_map, long ts) {
		this.storeMap = store_map;
		this.ts = ts;
		this.cleanFlagD = false;
	}

}
