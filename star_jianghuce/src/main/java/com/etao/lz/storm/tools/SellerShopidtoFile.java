package com.etao.lz.storm.tools;

import gnu.trove.map.hash.TLongLongHashMap;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class SellerShopidtoFile {

	// 店铺id->卖家id映射表
	static TLongLongHashMap shopId2SellerIdMap;

	static TLongLongHashMap shopId3SellerIdMap;

	public static void main(String[] args) throws Exception {

		shopId2SellerIdMap = new TLongLongHashMap();

		InputStream f = ClassLoader
				.getSystemResourceAsStream("shopid_sellerid.txt");

		String split = "-";

		String line = null;
		BufferedReader bf = new BufferedReader(new InputStreamReader(f));

		// shopid-sellerid
		try {
			while ((line = bf.readLine()) != null) {
				String[] lineArr = line.split(split, -1);
				long shopid = Long.parseLong(lineArr[1]);
				long sellerid = Long.parseLong(lineArr[0]);

				shopId2SellerIdMap.put(shopid, sellerid);

			}
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		if (bf != null)
			bf.close();

		FileOutputStream fout = new FileOutputStream("seller_shop_id_dim.ser");

		ObjectOutputStream bout = new ObjectOutputStream(fout);

		bout.writeObject(shopId2SellerIdMap);

		bout.flush();
		bout.close();

		System.out.println("thishdif sidf ok !!!!!!");

		// test------------------------------

		String sellerIdPath = "shop_seller_id_dim.ser";
		ObjectInputStream ois;
		try {
			if (sellerIdPath != null) {
				InputStream is = ClassLoader
						.getSystemResourceAsStream(sellerIdPath);
				if (is != null) {
					ois = new ObjectInputStream(is);
					shopId3SellerIdMap = (TLongLongHashMap) ois.readObject();
					ois.close();
					is.close();
				} else {
					System.out.println(1);
					return;
				}
			}
		} catch (ClassNotFoundException e) {

			System.out.println(2);

		} catch (IOException e) {
			System.out.println(3);
		}

		System.out.println(shopId3SellerIdMap.get(559595395L));

	}

}
