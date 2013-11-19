package com.etao.lz.star.bolt;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.etao.lz.star.StarConfig;
import com.etao.lz.star.StarLogProtos;
import com.etao.lz.star.utils.FormatLocation;
import com.etao.lz.star.utils.MysqlUtils;
import com.etao.lz.star.utils.ShopInfo;
import com.etao.lz.star.utils.TimerUtils;
import com.google.protobuf.GeneratedMessage.Builder;

public class LzdtEtlFillProcessor implements LogProcessor {

	
	private static final long serialVersionUID = 6616523998315239215L;
	/**
	 * 此类，主要处理店铺经trans_proc功能。
	 */
	
	private	FormatLocation formatLocation;
	private ConcurrentHashMap<String, ShopInfo> _shopInfos;
	private Log LOG = LogFactory.getLog(LzdtEtlFillProcessor.class);

	@Override
	public boolean accept(@SuppressWarnings("rawtypes") Builder builder) {
		StarLogProtos.FlowStarLog.Builder log = (StarLogProtos.FlowStarLog.Builder) builder;
		String shopid = log.getShopid();
		Pattern pattern = Pattern.compile("[0-9]+");
		if((pattern.matcher(shopid).matches()) && _shopInfos.containsKey(shopid)) {
			return true;
		}
		return false;
	}

	@Override
	public void config(StarConfig config) {
	}

	@Override
	public void open(int taskId) {
		_shopInfos =  MysqlUtils.getShopInfo();
		if(_shopInfos.isEmpty()){
			LOG.error("load  shopinfos  failed !!!");
			System.exit(1);
		}else
		{
			String len   = String.valueOf(_shopInfos.size());
		    LOG.info("load  shop  user  total numbers : "  + len);
		}
		TimerUtils.ShopInfoResetTimer(_shopInfos);
		formatLocation = new FormatLocation();
		try {
			formatLocation.init();
		    LOG.info("load ipmap lenth : "  + formatLocation.getIpmapLen());
		} catch (IOException e) {
			LOG.error("load ipmap  failed !!!");
			e.printStackTrace();

		}
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void process(Builder builder) {
		StarLogProtos.FlowStarLog.Builder log = (StarLogProtos.FlowStarLog.Builder) builder;
		
		String shopid = log.getShopid();
		
		ShopInfo ret = _shopInfos.get(shopid);
		long  user_id = ret.getUserId();
		String shop_type = ret.getShopType();
		String nick = ret.getNick();
		long  unit_id = ret.getUnitId();
		
		long location_id = 0;

		try {
			location_id = formatLocation.formatLocationid(log.getIp());
		} catch (Exception e) {
			LOG.info("获取异常ip ： " + log.getIp());
			e.printStackTrace();

		}
		
		String lzsession = log.getLinezingSession();
		String ss;
		int urlsn = 1;
		if(lzsession.equals("")) {
			Random random = new Random();
			MessageDigest md = null;
	        try {
	            md = MessageDigest.getInstance("MD5");
	        } catch (NoSuchAlgorithmException e) {
	            e.printStackTrace();
	        }
	        byte[] random_bytes = new byte[10];
	        random.nextBytes(random_bytes);
	        md.update(random_bytes);
	        byte[] bs = md.digest();
	        
	        StringBuffer sb = new StringBuffer();  
	        for(int i = 0; i < bs.length; i++) {
	            int v = bs[i]&0xff;  
	            if(v < 16) {  
	                sb.append(0);  
	            }
	            sb.append(Integer.toHexString(v));  
	        }  
			ss = sb.toString().substring(0, 24);
		} else {
			String[] ss_items = lzsession.split("_");
			ss = ss_items[0];
			urlsn = Integer.parseInt(ss_items[2]);
		}
		
		log.setUserId(user_id);
		log.setShopType(shop_type);
		log.setNickname(nick);
		log.setUnitId(unit_id);
		log.setLogStatus("120");
		log.setLocationId(location_id);
		log.setSs(ss);
		log.setUrlSn(urlsn);
	}

	@Override
	public void getStat(Map<String, String> map) {
		
	}

}
