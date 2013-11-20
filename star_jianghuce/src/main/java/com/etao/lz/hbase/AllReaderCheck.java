package com.etao.lz.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.NavigableMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.etao.lz.storm.tools.Constants;
import com.etao.lz.storm.utils.TimeUtils;





/*
 * 
 * 双十一宝贝销量直播，数据验证测试类
 * 
 * @paragm   sellerid  auctionid 
 * 
 * */




public class AllReaderCheck {
	
	

	// 牵牛2期  HBase表列簇名
	private static final byte[] columnFamily = Bytes.toBytes(Constants.BIGB_HBASE_FAMILY);
	
	
	private static final byte[] pv = Bytes.toBytes("pv");    // 浏览量     long/8
	private static final byte[] amt = Bytes.toBytes("amt")       ;// 支付宝成交金额        long/8
	private static final byte[] an  = Bytes.toBytes("an")        ;// 支付宝成交件数        long/8
	private static final byte[] tn  = Bytes.toBytes("tn")       ;//  拍下笔数              long/8
	private static final byte[] atn = Bytes.toBytes("atn")       ;// 成交笔数              long/8
	private static final byte[] auv = Bytes.toBytes("auv")       ;// 成交uv                long/8
	private static final byte[] iuv = Bytes.toBytes("iuv")       ;// 流量uv                long/8
	private static final byte[] ar = Bytes.toBytes("ar")       ;//  成交转化率（auv/iuv） long/8
	private static final byte[] az = Bytes.toBytes("az")       ;//  当日支付率（atn/tn）  long/8
	private static final byte[] au = Bytes.toBytes("au")       ;//  访客价值（amt/iuv）   long/8
	
	public static final String tablenameD = Constants.DP_D_TABLE;
	public static final String tablenameH = Constants.DP_H_TABLE;

	public static final Log log = LogFactory.getLog(AllReaderCheck.class);

	// HBase客户端配置对象
	private static Configuration conf;
	
	public static void readUnitDay(long sellerid, long auctionid, long ts) throws IOException{
		
		int date = Integer.parseInt(TimeUtils.getTimeDate(ts));
		byte[] key = RowUtils.RptDpt.rowKeyDay(date, sellerid, auctionid);
		
		Configuration cconf = new Configuration();
		cconf.addResource("hbase-site-new.xml");
		
		conf = HBaseConfiguration.create(cconf);
		
		HBaseConfiguration.addHbaseResources(conf);
		
	    HTable hTable = new HTable(conf, tablenameD);
		Get get = new Get(key);
		get.addFamily(columnFamily);
		
		try {
			
			Result re = hTable.get(get);
			
			if (re == null) {
			    log.error("[getUnitValues] hbase.result is null");
				return ;
			}
			if(re.isEmpty()) {
				log.error("[getUnitValues] hbase.result is empty");
				return ;
			}
		
			//从family层级 开始寻找colum , value
			NavigableMap<byte[], byte[]> map  = re.getFamilyMap(columnFamily);
			
			if (map == null || map.size() < 1) {
				log.error("[getUnitDValues] map.result is empty");
				return;
			}

			
			long hpv = Bytes.toLong(map.get(pv));
			long hamt = Bytes.toLong(map.get(amt));
			long han = Bytes.toLong(map.get(an));
			long htn = Bytes.toLong(map.get(tn));
			long hatn = Bytes.toLong(map.get(atn));
			long hauv = Bytes.toLong(map.get(auv));
			long hiuv = Bytes.toLong(map.get(iuv));
			long har = Bytes.toLong(map.get(ar));
			long haz = Bytes.toLong(map.get(az));
			long hau = Bytes.toLong(map.get(au));
				
			System.out.println("get D +++++++++++++++++++++++++++++");
			System.out.println("get  pv : " + String.valueOf(hpv));
			System.out.println("get  amt : " + String.valueOf(hamt));
			System.out.println("get  an : " + String.valueOf(han));
			System.out.println("get  tn : " + String.valueOf(htn));
			System.out.println("get  atn : " + String.valueOf(hatn));
			System.out.println("get  auv : " + String.valueOf(hauv));
			System.out.println("get  iuv : " + String.valueOf(hiuv));
			System.out.println("get  ar : " + String.valueOf(har));
			System.out.println("get  az : " + String.valueOf(haz));
			System.out.println("get  au : " + String.valueOf(hau));
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
		
	//从hbase中读取店铺的数据状态
	public  static void  readUnitHour(long sellerid, long auctionid, long ts) throws IOException {
		
		int date = Integer.parseInt(TimeUtils.getTimeDate(ts));
		System.out.println("time of hbase : " + date); 
		
	    HTable hTable = new HTable(conf, tablenameH);
		ArrayList<Get> getlist = new ArrayList<Get>();
		
		for(int i = 0; i < 24; i++) {
			
			byte timeinterval = (byte) i;
			
			byte[] key = RowUtils.RptDpt.rowKeyHour(date, timeinterval, sellerid, auctionid);
			
			Get get = new Get(key);
			
			get.addFamily(columnFamily);
			
			getlist.add(get);
			
		}
		
		try {
			Result[] re = hTable.get(getlist);
			
			if (re == null) {
			    log.error("[getUnitValues] hbase.result is null");
				return ;
			}
			
			if(re.length < 1) {
				log.error("[getUnitValues] hbase.result is empty");
				return ;
			}
		
			//从family层级 开始寻找colum , value
			long  tapv = 0;
			long  tamt = 0;
			long  tan = 0;
			long  ttn = 0;
			long  tatn = 0;
			long  tauv = 0;
			long  tiuv = 0;
			long  tar = 0;
			long  taz = 0;
			long  tau = 0;
			
			for(int j=0; j < re.length; j++) {
			
				NavigableMap<byte[], byte[]> map  = re[j].getFamilyMap(columnFamily);
				
				if (map == null || map.size() < 1) {
					log.error("[getHUnitValues] map.result is empty h : "  + j);
					continue;
				}

				long hpv = Bytes.toLong(map.get(pv));
				long hamt = Bytes.toLong(map.get(amt));
				long han = Bytes.toLong(map.get(an));
				long htn = Bytes.toLong(map.get(tn));
				long hatn = Bytes.toLong(map.get(atn));
				long hauv = Bytes.toLong(map.get(auv));
				long hiuv = Bytes.toLong(map.get(iuv));
				long har = Bytes.toLong(map.get(ar));
				long haz = Bytes.toLong(map.get(az));
				long hau = Bytes.toLong(map.get(au));
				
				 tapv +=  hpv;
				 tamt += hamt;
				 tan  += han;
				 ttn += htn;
				 tatn += hatn;
				 tauv += hauv;
				 tiuv += hiuv;
				 tar += har;
				 taz += haz;
				 tau += hau;
				
					
				System.out.println("-----------------------------");
				System.out.println("get H  : " + String.valueOf(j));
				System.out.println("get  pv : " + String.valueOf(hpv));
				System.out.println("get  amt : " + String.valueOf(hamt));
				System.out.println("get  an : " + String.valueOf(han));
				System.out.println("get  tn : " + String.valueOf(htn));
				System.out.println("get  atn : " + String.valueOf(hatn));
				System.out.println("get  auv : " + String.valueOf(hauv));
				System.out.println("get  iuv : " + String.valueOf(hiuv));
				System.out.println("get  ar : " + String.valueOf(har));
				System.out.println("get  az : " + String.valueOf(haz));
				System.out.println("get  au : " + String.valueOf(hau));
				
				
				
				
			}
		
			System.out.println("+++++++++++++++++++++++++++++");
			System.out.println("get  pv total: " + String.valueOf(tapv));
			System.out.println("get  amt total: " + String.valueOf(tamt));
			System.out.println("get  an total: " + String.valueOf(tan));
			System.out.println("get  tn total: " + String.valueOf(ttn));
			System.out.println("get  atn total: " + String.valueOf(tatn));
			System.out.println("get  auv total: " + String.valueOf(tauv));
			System.out.println("get  iuv total: " + String.valueOf(tiuv));
			System.out.println("get  ar total: " + String.valueOf(tar));
			System.out.println("get  az total: " + String.valueOf(taz));
			System.out.println("get  au total: " + String.valueOf(tau));
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	
	
	
	
	public  static void main(String [] args) throws IOException{
		
		if(args.length < 2){
			System.out.println("please input sellerid and auctionid: ");
			System.exit(0);
		}
		
		long sellerid  = Long.parseLong(args[0]);
		long auctionid = Long.parseLong(args[1]);
		long ts  =  System.currentTimeMillis() / 1000;
		
		System.out.println("sellerid : " + sellerid + " auction_id : " + auctionid);
		
		AllReaderCheck.readUnitDay(sellerid, auctionid, ts);
		AllReaderCheck.readUnitHour(sellerid, auctionid, ts);
		/*
		System.out.println(System.currentTimeMillis());
		System.out.println(TimeUtils.getCurrMinTime());
		System.out.println(TimeUtils.getTimeHour(1381477560L));
		System.out.println(Byte.parseByte("11"));
		BigbReaderCheck.readUnit(sellerid, ts);
 */
	}


}
