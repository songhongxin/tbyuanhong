package com.etao.lz.star.output;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.etao.lz.star.StarConfig;
import com.etao.lz.star.StarLogProtos;
import com.google.protobuf.GeneratedMessage;

public class LogtodiskLogOutput implements LogOutput {

	/*
	 * 
	 * 验证上游数据的准确性，打了一些本地化日志。
	 * 
	 * */

	private static final long serialVersionUID = -5988705921061064974L;
	private static Log LOG = LogFactory.getLog(LogtodiskLogOutput.class);
	@Override
	public void config(StarConfig config) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void open(int id) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void output(byte[] data, GeneratedMessage log) {
		
		StarLogProtos.FlowStarLog mes = (StarLogProtos.FlowStarLog) log;
		
		String  unit_id  =  String.valueOf(mes.getUnitId());
		if (unit_id.equals("54173")) {
			
			String log_time = mes.getLogTime();
			String ts = String.valueOf(mes.getTs());
			String shopid = mes.getShopid();
			String shop_type = mes.getShopType();
			String src_id = String.valueOf(mes.getSrcId());
			String ref_url = mes.getReferUrl();
			String agent = mes.getAgent();
			String nick_name = mes.getNickname();
			String ip = mes.getIp();
			String location_id = String.valueOf(mes.getLocationId());
			String acookie = mes.getMid();
			String user_id = mes.getUid();
			String user_nick = mes.getUserNick();
			String ss = mes.getSs();
			String url_sn = String.valueOf(mes.getUrlSn());
			String ref = mes.getReferUrl();
			String url = mes.getUrl();
			String log_status = mes.getLogStatus();
			String url_type = mes.getUrlType();
			String url_index = mes.getUrlIndex();
			String url_title = mes.getUrlTitle();
			String ref_baobei = mes.getRefBaobei();
			String key_type = mes.getKeyType();
			String keyword = mes.getKeyword();
			String url_return = String.valueOf(mes.getUrlReturn());
			String dp_return = String.valueOf(mes.getDpReturn());
			
			
			String  info = log_time + '"' + ts + '"' +
			               shopid + '"'  + shop_type + '"' +
			               src_id +  '"' +  ref_url + '"' +
			               agent +  '"'  + agent  + '"' +
			               nick_name + '"' + ip  + '"' +
			               location_id + '"' + acookie + '"' +
			               user_id;
			
			info  += '"' + user_nick;
			info  += '"' + ss;
			info  += '"' + url_sn;
			info  += '"' + ref;
			info  += '"' + url;
			info  += '"' + log_status;
			info  += '"' + url_type;
			info  += '"' + url_index;
			info  += '"' + url_title;
			info  += '"' + ref_baobei;
			info  += '"' + key_type;
			info  += '"' + keyword;
			info  += '"' + url_return;
			info  += '"' + dp_return;
			
			LOG.info(info);
			               		
			
		}
        
        
	}

	@Override
	public void getStat(Map<String, String> map) {
		// TODO Auto-generated method stub
		
	}

}
