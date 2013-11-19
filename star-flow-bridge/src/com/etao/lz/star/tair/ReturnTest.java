package com.etao.lz.star.tair;

import com.etao.lz.star.utils.AdaptiveCountingCheck;
import com.etao.lz.star.utils.GZipUtil;
import com.taobao.tair.ResultCode;
import com.taobao.tair.etc.TairConstant;
import com.taobao.tair.impl.mc.MultiClusterExtendTairManager;

public class ReturnTest {
	
	
	public static void main(String args[]){
		
		
		
		
		DpReturn    dpreturn   = new   DpReturn();
		
		
		String shopid   =  "12345678";
		
		String  cookie  = "Vk7bB+C6gTcCAebU43prArzJ";
		
		dpreturn.init();
		
		int   r  =  dpreturn.getDpReturn(shopid, cookie);
		
		
		System.out.println(r);
		
		
		AdaptiveCountingCheck   tom  =  new AdaptiveCountingCheck(TConfig.UV_BITMAP_K);
		
		tom.offer(cookie);
		
		
		
		MultiClusterExtendTairManager  tair = GetTairClient.getRdbClient();
		
		String rdb_key = String.format("%s_%s_return", shopid, 20130720);

		byte[] val = tom.getBytes();

		byte[] rdb_val = GZipUtil.GZip(val);

		ResultCode rcode = tair.put(TConfig.namespace, rdb_key, rdb_val,
				TairConstant.NOT_CARE_VERSION, TConfig.return_expire_time);

		if (rcode.isSuccess()) {

			 System.out.println("put ok !");

		} else {

			String errMes = String.format(
					"put  session_bit_map_rdb key : %s  not  success", rdb_key);
			System.out.println(errMes);
		}
		
		
		
		
		
		
		
		
		
		
		
		
	}

}
