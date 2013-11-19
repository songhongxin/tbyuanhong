package com.etao.lz.star.tair;

import com.etao.lz.star.utils.AdaptiveCountingCheck;
import com.etao.lz.star.tair.TConfig;

/*
 * 计算回头客用的数据结构
 * 
 * 
 * 
 * add by yuanhong.shx
 * */

public class ReturnValues {

	public AdaptiveCountingCheck historyvalue; // 6天前的历史数据，用来判定是否存在。

	public AdaptiveCountingCheck todayvalue; // 当天的回头客信息数据。

	public boolean needCommit = false; // 是否发生变化
	
	public ReturnValues() {

		this.historyvalue = new AdaptiveCountingCheck(TConfig.UV_BITMAP_K);
		this.todayvalue = new AdaptiveCountingCheck(TConfig.UV_BITMAP_K);

	}

}
