package com.etao.lz.storm.detailcalc;

import gnu.trove.map.hash.TLongObjectHashMap;

public class StoreItem {
	
	public long ts;    
	public TLongObjectHashMap<UnitItem> storeMap; 
	public boolean cleanFlagH;    
	public boolean cleanFlagD;    
	
	public StoreItem(TLongObjectHashMap<UnitItem> store_map, long ts) {
		this.storeMap = store_map;
		this.ts = ts;
		this.cleanFlagH = false;
		this.cleanFlagD = false;
	}
	
}