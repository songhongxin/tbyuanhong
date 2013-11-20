package com.etao.lz.storm.utils;

import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.log4j.Logger;

import com.etao.lz.star.StarLogProtos;

public class TimeUtil {

	// Biz时间格式
	private static SimpleDateFormat formatter = new SimpleDateFormat(
			"yyyy-MM-dd HH:mm:ss");

	// 输出日志
	private static Logger logger = Logger.getLogger(TimeUtil.class);

	/**
	 * 获得业务日志的有效时间戳。
	 * 
	 * @param biz
	 * @return
	 */
	public static long bizTimeToTs(StarLogProtos.BusinessStarLog biz) {
		try {
			// NOTE: SimpleDateFormatter 非线程安全，必须要加同步锁！
			synchronized (formatter) {
				if (biz.getIsPay() == 0) {
					if (biz.hasGmtModified()
							&& biz.getGmtModified().length() > 0)
						return formatter.parse(biz.getGmtModified()).getTime();
					else if (biz.hasGmtCreate()
							&& biz.getGmtCreate().length() > 0)
						return formatter.parse(biz.getGmtCreate()).getTime();
				} else {
					return formatter.parse(biz.getPayTime()).getTime();
				}
			}
		} catch (Exception e) {
			logger.warn("DateTime format error: " + e.getMessage());
			return 0; // 格式错误导致无法转换
		}
		logger.warn("Can not get business log's ts. GmtModified:"
				+ biz.getGmtModified()
				+ ", GmtCreate:"
				+ biz.getGmtCreate()
				+ ", PayTime:"
				+ biz.getPayTime());
		return 0;
	}

	/**
	 * NOTE: 外部成交的时间和站内成交的不同！
	 */
	public static long exBizTimeToTs(StarLogProtos.BusinessStarLog biz) {
		try {
			// NOTE: SimpleDateFormatter 非线程安全，必须要加同步锁！
			synchronized (formatter) {
				if (biz.hasGmtModified()
						&& biz.getGmtModified().length() > 0
						&& !biz.getGmtModified().equalsIgnoreCase("\\N"))
					return formatter.parse(biz.getGmtModified()).getTime();
				else if (biz.hasGmtCreate()
						&& biz.getGmtCreate().length() > 0
						&& !biz.getGmtCreate().equalsIgnoreCase("\\N"))
					return formatter.parse(biz.getGmtCreate()).getTime();
			}
		} catch (Exception e) {
			logger.warn("DateTime format error: " + e.getMessage());
			return 0; // 格式错误导致无法转换
		}
		logger.warn("Can not get business log's ts. GmtModified:"
				+ biz.getGmtModified()
				+ ", GmtCreate:"
				+ biz.getGmtCreate()
				+ ", PayTime:"
				+ biz.getPayTime());
		return 0;
	}
	
	/**
	 * NOTE: 获取当前时间的小时信息
	 */
	public static String getTodayHour() {

		try {
			// NOTE: SimpleDateFormatter 非线程安全，必须要加同步锁！
			synchronized (formatter) {

				Calendar curTime = Calendar.getInstance(); // today time
				String todayDate = formatter.format(curTime.getTime()); // today
																		// string
				String timeDate = todayDate.substring(11, 13); // day date
				return timeDate;

			}
		} catch (Exception e) {
			logger.warn("geth  DateTime format error: " + e.getMessage());
			return "24"; // 格式错误导致无法转换
		}

	}
	
	/*
	public  static  void  main(String [] args){
		
		
		System.out.println(getTodayHour());
	}
	*/
}

