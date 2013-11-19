package com.etao.lz.star.utils;


import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;


public class TimeUtils {
	

	public static long getTodayTimeBegin() {
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss"); // time format
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
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd HH-mm-ss"); // time format
		Calendar curTime = Calendar.getInstance(); // today time
		String todayDate = formatter.format(curTime.getTime()); // today string
		String timeDate = todayDate.substring(0, 8); // day date
		return timeDate;
		
	}
	
	public static String getTodayHour() {
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd HH-mm-ss"); // time format
		Calendar curTime = Calendar.getInstance(); // today time
		String todayDate = formatter.format(curTime.getTime()); // today string
		String timeDate = todayDate.substring(9, 11); // day date
		return timeDate;
		
	}
	
	public static String getTodayDayAgo(int i) {
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd HH-mm-ss"); // time format
		Calendar curTime = Calendar.getInstance(); // today time
		curTime.add(Calendar.DAY_OF_MONTH, -i); // before time
		String todayDate = formatter.format(curTime.getTime()); // today string
		String timeDate = todayDate.substring(0, 8); // day date
		return timeDate;
		
	}
	
	

	public static long getTomorrowTimeBegin() {
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss"); // time format
		Calendar curTime = Calendar.getInstance(); // today time
		curTime.add(Calendar.DAY_OF_MONTH, 1); // tomorrow time

		String tomorrowDate = formatter.format(curTime.getTime()); // tomorrow string
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
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss"); // time format
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
	
	public static String getTimeHour(long timestamp) {
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss"); // time format

	    Date  time  = new  Date(new Long(timestamp) * 1000);
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
		return (getTodayTimeBegin() == lastTime)?true:false;
	}

	/*
	public static void main(String[] args) throws Exception {
		
		

		  System.out.println(getTomorrowTimeBegin());
		
	
	       SimpleDateFormat format =   new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss" );

	       Date  time  = new  Date(new Long(1358352000) * 1000);
	       String d = format.format(time);
	       System.out.println(d); 
	       Date date=format.parse(d);

	       System.out.println("Format To String(Date):"+d);

	       System.out.println("Format To Date:"+date); 
	       System.out.println(getTodayDay()); 
	       System.out.println(getTimeHour(1358352000)); 

		
	}
	*/
	
	
	
}
