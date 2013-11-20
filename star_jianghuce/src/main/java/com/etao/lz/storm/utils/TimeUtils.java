package com.etao.lz.storm.utils;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.log4j.Logger;

import com.etao.lz.star.StarLogProtos;

public class TimeUtils {
	// 输出日志
	private static Logger logger = Logger.getLogger(TimeUtils.class);

	public static long getTodayTimeBegin() {
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss"); // time
																					// format
		Calendar curTime = Calendar.getInstance(); // today time
		String todayDate = formatter.format(curTime.getTime()); // today string
		String timeDate = todayDate.substring(0, 10); // day date
		timeDate = timeDate + " 00-00-00"; // today 00:00:00

		Date dateBegin = null;
		try {
			dateBegin = formatter.parse(timeDate);
		} catch (Exception e) {
			e.printStackTrace();
		}

		long dayBegin = 0L;
		if (dateBegin != null) {
			dayBegin = dateBegin.getTime();
		}
		return dayBegin;

	}

	public static String getTodayDay() {
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd HH-mm-ss"); // time
																				// format
		Calendar curTime = Calendar.getInstance(); // today time
		String todayDate = formatter.format(curTime.getTime()); // today string
		String timeDate = todayDate.substring(0, 8); // day date
		return timeDate;

	}

	public static String getTodayHour() {
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd HH-mm-ss"); // time
																				// format
		Calendar curTime = Calendar.getInstance(); // today time
		String todayDate = formatter.format(curTime.getTime()); // today string
		String timeDate = todayDate.substring(9, 11); // day date
		return timeDate;

	}

	public static String getTodayDayAgo(int i) {
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd HH-mm-ss"); // time
																				// format
		Calendar curTime = Calendar.getInstance(); // today time
		curTime.add(Calendar.DAY_OF_MONTH, -i); // before time
		String todayDate = formatter.format(curTime.getTime()); // today string
		String timeDate = todayDate.substring(0, 8); // day date
		return timeDate;

	}

	public static long getTomorrowTimeBegin() {
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss"); // time
																					// format
		Calendar curTime = Calendar.getInstance(); // today time
		curTime.add(Calendar.DAY_OF_MONTH, 1); // tomorrow time

		String tomorrowDate = formatter.format(curTime.getTime()); // tomorrow
																	// string
		String timeDate = tomorrowDate.substring(0, 10); // tomorrow date
		timeDate = timeDate + " 00-00-00"; // tomorrow 00:00:00

		Date dateBegin = null;
		try {
			dateBegin = formatter.parse(timeDate);
		} catch (Exception e) {
			e.printStackTrace();
		}

		long dayBegin = 0L;
		if (dateBegin != null) {
			dayBegin = dateBegin.getTime();
		}
		return dayBegin;
	}

	public static long getCurrMinTime() {
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss"); // time
																					// format
		Calendar curTime = Calendar.getInstance(); // today time
		String todayDate = formatter.format(curTime.getTime()); // today string
		String timeDate = todayDate.substring(0, 17); // day date
		timeDate = timeDate + "00"; // current minute

		Date dateBegin = null;
		try {
			dateBegin = formatter.parse(timeDate);
		} catch (Exception e) {
			e.printStackTrace();
		}

		long dayBegin = 0L;
		if (dateBegin != null) {
			dayBegin = dateBegin.getTime();
		}
		return dayBegin;
	}

	/*
	 * 获取时间延迟n分钟的最后一秒时间戳
	 */
	public static long getTimeCurMax(long timestamp, int n) {
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd HH-mm-ss"); // time
																				// format

		Date time = new Date(timestamp * 1000L);
		String d = formatter.format(time);
		String date = d.substring(0, 15); // day date
		date += "59";

		Date datetmp = null;
		try {
			datetmp = formatter.parse(date);
		} catch (Exception e) {
			e.printStackTrace();
		}
		long finaldate = 0L;
		if (datetmp != null) {
			finaldate = (datetmp.getTime() / 1000) + (n - 1) * 60;
		}

		return finaldate;
	}

	/*
	 * 获取时间戳本天第一秒时间戳
	 */
	public static long getTimeCurDayMin(long timestamp) {
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd HH-mm-ss"); // time
																				// format

		Date time = new Date(timestamp * 1000L);
		String d = formatter.format(time);
		String date = d.substring(0, 9); // day date
		date += "00-00-00";

		Date datetmp = null;
		try {
			datetmp = formatter.parse(date);
		} catch (Exception e) {
			e.printStackTrace();
		}
		long finaldate = 0L;
		if (datetmp != null) {
			finaldate = datetmp.getTime() / 1000;
		}

		return finaldate;
	}

	/*
	 * 获取时间戳本天最后一秒时间戳
	 */
	public static long getTimeCurDayMax(long timestamp) {
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd HH-mm-ss"); // time
																				// format

		Date time = new Date(timestamp * 1000L);
		String d = formatter.format(time);
		String date = d.substring(0, 9); // day date
		date += "23-59-59";

		Date datetmp = null;
		try {
			datetmp = formatter.parse(date);
		} catch (Exception e) {
			e.printStackTrace();
		}
		long finaldate = 0L;
		if (datetmp != null) {
			finaldate = datetmp.getTime() / 1000;
		}

		return finaldate;
	}

	/*
	 * 获取时间戳本小时最后一秒时间戳
	 */
	public static long getTimeCurHourMax(long timestamp) {
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd HH-mm-ss"); // time
																				// format

		Date time = new Date(timestamp * 1000L);
		String d = formatter.format(time);
		String date = d.substring(0, 12); // day date
		date += "59-59";

		Date datetmp = null;
		try {
			datetmp = formatter.parse(date);
		} catch (Exception e) {
			e.printStackTrace();
		}
		long finaldate = 0L;
		if (datetmp != null) {
			finaldate = datetmp.getTime() / 1000;
		}

		return finaldate;
	}

	/*
	 * 获取时间戳的日期
	 */
	public static String getTimeDate(long timestamp) {
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd HH-mm-ss"); // time
																				// format

		Date time = new Date(timestamp * 1000L);
		String d = formatter.format(time);
		String date = d.substring(0, 8);

		return date;
	}

	/*
	 * 获取时间戳的日期
	 */
	public static String getTimeDatePattern2(long timestamp) {
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss"); // time
																					// format

		Date time = new Date(timestamp * 1000L);
		String d = formatter.format(time);
		String date = d.substring(0, 10);

		return date;
	}

	public static String getTimeHour(long timestamp) {
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss"); // time
																					// format

		Date time = new Date(timestamp * 1000L);
		String d = formatter.format(time);

		String Hour = d.substring(11, 13);

		return Hour;
	}

	public static long getNextMinTime() {
		return getCurrMinTime() + 1000 * 60;
	}

	public static long getPrevMinTime() {
		return getCurrMinTime() - 1000 * 60;
	}

	public static long getPrevMinTime(long lastTime) {
		return lastTime - 1000 * 60;
	}

	public static boolean isTodayFirstMinute(long lastTime) {
		return (getTodayTimeBegin() == lastTime) ? true : false;
	}

	/**
	 * 获得业务日志的有效时间戳。
	 * 
	 * @param biz
	 * @return
	 */
	public static long bizTimeToTs(StarLogProtos.BusinessStarLog biz) {

		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss"); // time
																					// format
		try {
			if (biz.getIsPay() == 0) {
				if (biz.hasGmtModified() && biz.getGmtModified().length() > 0)
					return formatter.parse(biz.getGmtModified()).getTime();
				else if (biz.hasGmtCreate() && biz.getGmtCreate().length() > 0)
					return formatter.parse(biz.getGmtCreate()).getTime();
			} else {
				return formatter.parse(biz.getPayTime()).getTime();
			}
		} catch (Exception e) {
			logger.warn("DateTime format error: " + e.getMessage());
			return 0; // 格式错误导致无法转换
		}
		logger.warn("Can not get business log's ts. GmtModified:"
				+ biz.getGmtModified() + ", GmtCreate:" + biz.getGmtCreate()
				+ ", PayTime:" + biz.getPayTime());
		return 0;
	}

	public static void main(String[] args) throws Exception {

		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		// Date time = new Date(new Long(1358352000) * 1000);
		long x = getCurrMinTime();
		String d = format.format(x);
		int date = Integer.parseInt(getTimeDate(x / 1000));
		System.out.println(d);
		System.out.println(date);

		// long n = getTimeCurHourMax(1358352000);
		// time = new Date(n * 1000);
		// d = format.format(time);
		// System.out.println(d);

	}

}
