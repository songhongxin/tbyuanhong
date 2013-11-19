package com.etao.lz.star.output;

import junit.framework.Assert;

import org.junit.Test;

import com.etao.lz.star.StarConfig;
import com.etao.lz.star.StarLogProtos;
import com.etao.lz.star.StarLogProtos.FlowStarLog;
import com.etao.lz.star.StarLogProtos.FlowStarLog.Builder;
import com.etao.lz.star.output.broadcast.SendDataThread;
import com.etao.lz.star.output.broadcast.SubscriberInfo;

public class SubscriberInfoTest {

	@Test
	public void test() {
		SubscriberInfo item = new SubscriberInfo("test", "test");
		item.setFilter("$log.shopid != '' and $log.destType == 'browse'");
		item.setProjection(new String[]{"shopid","ts"});
		StarConfig config = new StarConfig("test");
		config.addAttribute("LogType", "flow");
		SendDataThread info = new SendDataThread(item, config);
		Assert.assertEquals(info.getSubscriberInfo().getProjection().size(), 2);
		Builder builder = StarLogProtos.FlowStarLog.newBuilder();
		
		FlowStarLog log = builder.build();
		info.addData(new byte[]{1,2,3}, log);
		Assert.assertEquals(info.getQueue().size() ,0);
		
		builder.setShopid("123");
		builder.setDestType("aaa");
		log = builder.build();
		info.addData(new byte[]{1,2,3}, log);
		Assert.assertEquals(info.getQueue().size() ,0);
		
		builder.setShopid("");
		builder.setDestType("aaa");
		log = builder.build();
		info.addData(new byte[]{1,2,3}, log);
		Assert.assertEquals(info.getQueue().size() ,0);
		
		builder.setShopid("123");
		builder.setDestType("browse");
		log = builder.build();
		info.addData(new byte[]{1,2,3}, log);
		Assert.assertEquals(info.getQueue().size() ,1);
	}

}
