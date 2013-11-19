package com.etao.lz.star.bolt;

import java.util.Map;

import com.etao.lz.star.StarConfig;
import com.etao.lz.star.StarLogProtos;
import com.etao.lz.star.tair.DpReturn;
import com.google.protobuf.GeneratedMessage.Builder;

@SuppressWarnings("rawtypes")
public class LzreturnlFillProcessor implements LogProcessor{
	
	private static final long serialVersionUID = 5741528717624527652L;
	/**
	 * 此类,主要计算用户是否是回头客的功能。
	 */
    
	private static DpReturn  dpreturn   =  new DpReturn();
	@Override
	public boolean accept(Builder builder) {
		StarLogProtos.FlowStarLog.Builder log = (StarLogProtos.FlowStarLog.Builder) builder;
		
		return log.getUnitId() > 0;
	}

	@Override
	public void config(StarConfig config) {

	}

	@Override
	public void open(int taskId) {
		
		dpreturn.init();

	}

	@Override
	public void process(Builder builder) {
		StarLogProtos.FlowStarLog.Builder log = (StarLogProtos.FlowStarLog.Builder) builder;
		
		String shopid = log.getShopid();  //店铺id
		String cookie = log.getMid();    // cna 
		
		
		if(shopid.equals("") || cookie.equals(""))
		{
			return;
		}
		
		int   dp_flag  =  get_return(shopid,cookie);   //获取回头客信息
		
		log.setDpReturn(dp_flag);
		log.setUrlReturn(0);

	}

	@Override
	public void getStat(Map<String, String> map) {
	}

	
	//获取回头客信息
    public  int  get_return(String shopid,  String cookie ){
    	
    	return  dpreturn.getDpReturn(shopid, cookie);
    }
	
	
	

}
