package com.etao.lz.storm.utils;

import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.map.hash.TLongObjectHashMap;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.mortbay.log.Log;

import com.etao.lz.storm.detailcalc.ShopItem;
import com.etao.lz.storm.tools.Constants;

public class MysqlUtil {

	public static Connection getConnectionByJDBC(String Mysql_Host,
			String Mysql_User, String Mysql_Passwd) {
		Connection conn = null;
		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			return conn;
		}
		try {
			conn = DriverManager.getConnection(Mysql_Host, Mysql_User,
					Mysql_Passwd);
		} catch (SQLException e) {
			e.printStackTrace();
			return conn;
		}
		return conn;
	}

	// 从mysql获取店铺指标数据
	public static void getShopDayMap(TLongObjectHashMap<ShopItem> unitMap,
			long ts) {

		Connection conn = getConnectionByJDBC(Constants.MYSQLHOST,
				Constants.MYSQLUSER, Constants.MYSQLPASSWORD);

		if (conn == null) {
			System.out.println("alizhg  fail to connect  mysql!");
			return;
		}

		try {
			Statement stmt = conn.createStatement();

			String date = TimeUtils.getTimeDatePattern2(ts);
			String sql = "select * from lz_jhc_shop_d where date = '" + date
					+ "'";
			ResultSet rs = stmt.executeQuery(sql);

			while (rs.next()) {
				long seller_id = rs.getLong("seller_id");
				ShopItem shopItem = new ShopItem(seller_id);

				// Log.info("get lz_jhc_shop_d sellerid , is " + seller_id);
				shopItem.alipayAmt = rs.getLong("alipay_amt");
				shopItem.px = shopItem.alipayAmt
						/ Constants.ALIPAYAMT_EVENT_BASE;
				unitMap.put(seller_id, shopItem);
			}
			Log.info("get lz_jhc_shop_d done , size is " + unitMap.size());

			stmt.close();

		} catch (SQLException e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		} finally {
			// 预防性关闭连接（避免异常发生时在try语句块关闭连接没有执行)
			try {
				conn.close();
			} catch (SQLException e) {
				System.out.println(e.getMessage());
				e.printStackTrace();
			}
		}

	}

	// 向mysql写入数据
	public static void writeShopDayRes(TLongObjectHashMap<ShopItem> unitMap,
			long ts) {

		Connection conn = getConnectionByJDBC(Constants.MYSQLHOST,
				Constants.MYSQLUSER, Constants.MYSQLPASSWORD);

		if (conn == null) {
			System.out.println("alizhg  fail to connect  mysql!");
			return;
		}

		try {

			PreparedStatement replacePS = conn
					.prepareStatement("REPLACE INTO lz_jhc_shop_d values (?,?,?)");
			PreparedStatement insertPS = conn
					.prepareStatement("INSERT INTO lz_jhc_shop_history(`seller_id`,`alipay_amt`,`timestamp`) values (?,?,?)");

			String date = TimeUtils.getTimeDatePattern2(ts);
			long sellerid, amt;
			long timestamp;
			ShopItem shopItem;

			for (TLongObjectIterator<ShopItem> itr = unitMap.iterator(); itr
					.hasNext();) {
				itr.advance();
				sellerid = itr.key();
				shopItem = itr.value();
				amt = shopItem.alipayAmt;

				replacePS.setLong(1, sellerid);
				replacePS.setLong(2, amt);
				replacePS.setString(3, date);
				replacePS.executeUpdate();
				// System.out.println(sellerid);

				if (shopItem.updateHistoryflag) {
					timestamp = shopItem.ts;
					insertPS.setLong(1, sellerid);
					insertPS.setLong(2, amt);
					insertPS.setLong(3, timestamp);
					insertPS.executeUpdate();
				}
			}
			replacePS.close();
			insertPS.close();

		} catch (SQLException e) {
			System.out.println(e.getMessage());
			Log.info("Sql error.");
			e.printStackTrace();
		} finally {
			// 预防性关闭连接（避免异常发生时在try语句块关闭连接没有执行)
			try {
				conn.close();
			} catch (SQLException e) {
				System.out.println(e.getMessage());
				e.printStackTrace();
			}
		}

	}

	/*
	 * public static void main(String[] args) {
	 * System.out.println("test mysql writer start");
	 * 
	 * TLongObjectHashMap<ShopItem> t = new TLongObjectHashMap<ShopItem>();
	 * 
	 * long sellerid = 1; ShopItem s = new ShopItem(sellerid); s.alipayAmt =
	 * 121211230;
	 * 
	 * long ts = Time.currentTimeMillis() / 1000; s.ts = ts; t.put(sellerid, s);
	 * 
	 * sellerid = 2; s = new ShopItem(sellerid); s.alipayAmt = 12230;
	 * 
	 * s.ts = ts; s.updateHistoryflag = true; t.put(sellerid, s);
	 * 
	 * sellerid = 3; s = new ShopItem(sellerid); s.alipayAmt = 412230;
	 * 
	 * s.ts = ts; s.updateHistoryflag = true; t.put(sellerid, s);
	 * 
	 * 
	 * writeShopDayRes(t, ts); //getShopDayMap(t, ts);
	 * System.out.println("test mysql writer end"); }
	 */

	public static void main(String[] args) {

		TLongObjectHashMap<ShopItem> unitMap = new TLongObjectHashMap<ShopItem>();
		long ts = 1383753390L;
		getShopDayMap(unitMap, ts);

	}

}