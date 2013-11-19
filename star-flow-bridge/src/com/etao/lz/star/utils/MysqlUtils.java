package com.etao.lz.star.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ConcurrentHashMap;

public class MysqlUtils {

	//public final static String Mysql_host = "jdbc:mysql://my065112.cm4:3306/lz_main";
	//public final static String Mysql_passwd = "lzb2c!@#";
//	public final static String Mysql_host = "jdbc:mysql://10.235.160.137:3306/lz_main";
	public final static String Mysql_host = "jdbc:mysql://my163035.cm6:3306/lz_main";
	public final static String Mysql_passwd = "H172hG2BfdqhdGqhd";
	public final static String Mysql_user = "lz_main_r";
	
	public static Connection getConnectionByJDBC(String Mysql_Host, String Mysql_User, String Mysql_Passwd) {
		Connection conn = null;
		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			return conn;
		//	System.exit(0);
		}
		try {
			/**
			 * 建立jdbc连接，但要注意此方法的第一个参数， 如果127.0.0.1出现CommunicationsException异常，
			 * 可能就需要改为localhost才可以
			 * jdbc:mysql://localhost:3306/test，test是数据库
			 **/
			conn = DriverManager.getConnection(
					Mysql_Host, Mysql_User, Mysql_Passwd);
		} catch (SQLException e) {
			e.printStackTrace();
			return conn;
		//	System.exit(0);
		}
		return conn;
	}

	public static ConcurrentHashMap<String, ShopInfo> getShopInfo() {
		ConcurrentHashMap<String, ShopInfo> map = new ConcurrentHashMap<String, ShopInfo>();
		
		String sql = "select nick_name as nick, shop_id, shop_type, user_id, unit_id from xiaoqiao_shop_info where is_active=1 and (init_time+days*86400)>unix_timestamp()";
		Connection conn = getConnectionByJDBC(Mysql_host, Mysql_user, Mysql_passwd);
		
		if(conn  == null)
			return map;
		
		try {
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			
			while (rs.next()) {
				ShopInfo shop = new ShopInfo();
				shop.setNick(rs.getString("nick"));
				shop.setShopId(rs.getString("shop_id"));
				shop.setShopType(rs.getString("shop_type"));
				shop.setUnitId(Long.parseLong(rs.getString("unit_id")));
				shop.setUserId(Integer.parseInt(rs.getString("user_id")));
				map.put(rs.getString("shop_id"), shop);
			}
		} catch (SQLException e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
			return map;
	//		System.exit(0);
		} finally {
			// 预防性关闭连接（避免异常发生时在try语句块关闭连接没有执行)
			try {
				if (conn != null)
					conn.close();
			} catch (SQLException e) {
				System.out.println(e.getMessage());
				e.printStackTrace();
				return map;
		//		System.exit(0);
			}
		}
		
		return map;
	}
	
	/*
	public static void main(String[] args) {
		System.out.println("123");
		ConcurrentHashMap<String, ShopInfo> res = MysqlUtils.getShopInfo();
		ShopInfo one = (ShopInfo) res.get("1");
		System.out.println(one.getUserId());
		TimerUtils.ShopInfoResetTimer(res);
	}
	*/
}