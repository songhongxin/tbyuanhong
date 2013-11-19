package com.etao.lz.star.tair;


import com.etao.lz.star.tair.TConfig;
import com.taobao.tair.impl.mc.MultiClusterExtendTairManager;

public class GetTairClient {
	
	
	public static MultiClusterExtendTairManager getRdbClient() {
		
	     MultiClusterExtendTairManager mcTairManager = new MultiClusterExtendTairManager();
	     mcTairManager.setConfigID(TConfig.ConfigId); //对应diamond上的dataid，从审批结果中获取
	     mcTairManager.setDynamicConfig(true);  //非常重要，不要忘记
	     mcTairManager.init();
	     
	     
		return mcTairManager;
	    
	     
	   }


}
