package com.etao.lz.star.tair;

public class TConfig {
	
	public static final short namespace = 767;
	public static final String  ConfigId= "ldbcount";   //生产环境
	//public static final String  ConfigId= "ldbcommon-daily";  //日常环境
	public static final short  UV_BITMAP_K = 13;
	public static final int  shop_slef = 5;
	public static final short  src_unknown = 3;
	
	public static final int  commit_time = 1000 * 60;
	public static final int  commit_pv = 1000 * 20;    // 回头客保存状态的时间间隔
	
	public static final long  delete_pv = 100;    // 清除用户
	

	
	// tair   参数 设置
	public static final int  expire_time = 36 * 60 *  60 ;  //一天半
	public static final int  return_expire_time = 6 * 24 * 60 *  60 ;  //6天
	
	public static final int  hour_expire_time =  60 *  60 ;  //一小时
	
	//public static final int  version = 0 ;
	//TairConstant.NOT_CARE_VERSION   建议用这个。
	public static final int  Locate_Top = 1000;  //保存地域数量
	public static final int  Src_Top = 500;     // 保存来源数量
	public static final int  Save_Status = 10;    // 保存非店铺状态时间间隔
	
	public static final int  Tair_limit = 8190;    // 保存店铺状态的上限
	
	
	
    public static final  long  unknown_cid = 65536;  //国家未知地区
    public static final  long  unknown_pid = 65792;  //省未知地区
    public static final  long  unknown_did = 65793;  //市未知地区
    
    
    

}
