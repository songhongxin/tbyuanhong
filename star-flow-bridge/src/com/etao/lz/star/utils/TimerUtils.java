package com.etao.lz.star.utils;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class TimerUtils {
	
	private static Log LOG = LogFactory.getLog(TimerUtils.class);

	public static void ShopInfoResetTimer(final ConcurrentHashMap<String, ShopInfo> shopInfos) {
		TimerTask task = new TimerTask() {
			@Override
			public void run() {
				synchronized(shopInfos) {
					try {
						ConcurrentHashMap<String, ShopInfo> newShopInfos = MysqlUtils.getShopInfo();
						if(!newShopInfos.isEmpty()) {
							String len   = String.valueOf(newShopInfos.size());
						    LOG.info("load  shop  user  total numbers : "  + len);
							shopInfos.clear();
							shopInfos.putAll(newShopInfos);	
						}else{
							
							 LOG.error("timer process  reload shopinfo  failled!");
				
						}
					} catch (Exception e) {
						e.getStackTrace().toString();
					}
				}
			}
		};

		long tomorrowBegin = TimeUtils.getTomorrowTimeBegin();
		long firstInterval = tomorrowBegin - System.currentTimeMillis();
// 每隔两个小时重新加载一次
		if (tomorrowBegin != 0) {
			Timer timer = new Timer();
			timer.schedule(task, firstInterval + 1000, 2 * (3600 + 1) * 1000);
		//	timer.schedule(task, 1000, 2 * (600 + 1) * 1000);
		}
	}
	
}