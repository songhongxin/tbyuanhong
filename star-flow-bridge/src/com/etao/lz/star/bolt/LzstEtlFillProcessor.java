package com.etao.lz.star.bolt;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.etao.lz.star.StarConfig;
import com.etao.lz.star.StarLogProtos;
import com.google.protobuf.GeneratedMessage.Builder;

@SuppressWarnings("rawtypes") 
public class LzstEtlFillProcessor implements LogProcessor{
	

	private static final long serialVersionUID = -5620328990882774851L;
	/**
	 * 此类，主要处理店铺经wget功能。
	 * 如，来源识别，被访页面识别等功能。
	 */
	private static Log LOG = LogFactory.getLog(LzstEtlFillProcessor.class);
	private static final String KEY_TAOBAO_P4P = "2"; // “p4p”直通车推广关健词
	private static final String KEY_TAOBAO_SHOP = "3"; // 淘宝店内搜索关健词
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

		LOG.info("LzstETL  processer  has  begined !");
	}

	@Override
	public void process(Builder builder) {
		StarLogProtos.FlowStarLog.Builder log = (StarLogProtos.FlowStarLog.Builder) builder;
		
		String shopid = log.getShopid();
		String title = log.getTitle();
		String url = log.getUrl();
		
		if(shopid.equals("") || title.equals("") || title.equals(""))
		{
			return;
		}
	//	LOG.info("get aplus  shopid is : " + shopid);
		String  ref = log.getReferUrl();
		
		title  = MergeParser.urldecode(title);
		HashMap<String,String>  merge_ret =  MergeParser.getInfo(url, title);
		
		HashMap<String,String>  ref_ret =    NewRefParser.getInfo(url, ref, shopid);
		
		log.setSrcId(Integer.parseInt(ref_ret.get("src_id")));
		log.setRefUrl(ref_ret.get("host"));
		
		log.setUrlType(merge_ret.get("page_type"));
		log.setUrlIndex(merge_ret.get("key_info"));
		log.setUrlTitle(merge_ret.get("title_info"));
		
		
		if(merge_ret.get("page_type").equals(KEY_TAOBAO_SHOP))
		{
			if(ref_ret.get("key_type").equals(KEY_TAOBAO_P4P))
			{
				log.setKeyType(ref_ret.get("key_type"));
				log.setKeyword(ref_ret.get("key"));
				
			}else
			{
				log.setKeyType(merge_ret.get("key_type"));
				log.setKeyword(merge_ret.get("key"));
			}
			
		}else
		{
			log.setKeyType(ref_ret.get("key_type"));
			log.setKeyword(ref_ret.get("key"));
		}
		
		

	}

	@Override
	public void getStat(Map<String, String> map) {
	}

	

	

}
