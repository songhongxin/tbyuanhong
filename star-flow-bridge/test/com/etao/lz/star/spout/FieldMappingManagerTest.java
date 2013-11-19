package com.etao.lz.star.spout;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

import com.etao.lz.star.StarLogProtos.BusinessStarLog;
import com.etao.lz.star.StarLogProtos.FlowStarLog;
import com.etao.lz.star.StarLogProtos.FlowStarLog.Builder;
import com.etao.lz.star.Tools;
import com.etao.lz.star.spout.Aplus.AplusLog;
import com.google.protobuf.ByteString;

public class FieldMappingManagerTest {

	@Test
	public void test() throws LogFormatException, UnsupportedEncodingException {
		LogMapping logMappings = new LogMapping();
		List<LogMappingItem> mappings = new ArrayList<LogMappingItem>();
		logMappings.put("tt_flow_spout", mappings);
		mappings.add(new LogMappingItem("log_version", "@int 1 1"));
		mappings.add(new LogMappingItem("log_time", "@field 1"));
		mappings.add(new LogMappingItem("adid", "@field2 2 2"));
		mappings.add(new LogMappingItem("mid", "@field3 2 3 3"));
		
		FieldMappingManager manager = new FieldMappingManager("flow", logMappings );
		Builder builder = FlowStarLog.newBuilder();
		manager.setFields(new String[]{"20120801121212", "123\001456\001a\003b\003c"}, new String[]{"\002","\001", "\003"});
		manager.doFieldMapping("tt_flow_spout", builder);
		Assert.assertEquals(builder.getLogVersion(), 1);
		Assert.assertEquals(builder.getLogTime(),"20120801121212");
		Assert.assertEquals(builder.getAdid(),"456");
		Assert.assertEquals(builder.getMid(),"c");
		
		mappings = new ArrayList<LogMappingItem>();
		logMappings.put("lz_etui_b2c_aplus_log", mappings);
		mappings.add(new LogMappingItem("log_version", "@int 2"));
		mappings.add(new LogMappingItem("uid", "@aplus_bytes uid"));
		manager = new FieldMappingManager("flow", logMappings );
		builder = FlowStarLog.newBuilder();
		AplusLog.Builder aplusBuilder = AplusLog.newBuilder();
		aplusBuilder.setVersion(ByteString.copyFrom("1".getBytes("utf-8")));
		aplusBuilder.setIp(123);
		aplusBuilder.setTime(456);
		aplusBuilder.setUrl(ByteString.copyFrom("http://aaaaaaaaaa".getBytes("utf-8")));
		aplusBuilder.setUserAgent(ByteString.copyFrom("nnnnnnnnnnnnnnnn".getBytes("utf-8")));
		aplusBuilder.setUid(ByteString.copyFrom("1234".getBytes("utf-8")));
		AplusLog aplus = aplusBuilder.build();
		
		manager.setAplus(aplus );
		manager.doFieldMapping("lz_etui_b2c_aplus_log", builder);
		Assert.assertEquals(builder.getLogVersion(), 2);
		Assert.assertEquals(builder.getUid(),"1234");
		
	}
	@Test
	public void test1() throws LogFormatException, UnsupportedEncodingException {
		LogMapping logMappings = new LogMapping();
		List<LogMappingItem> mappings = new ArrayList<LogMappingItem>();
		logMappings.put("tt_flow_spout", mappings);
		mappings.add(new LogMappingItem("mid", "@field3 2 0 3"));
		
		FieldMappingManager manager = new FieldMappingManager("flow", logMappings );
		Builder builder = FlowStarLog.newBuilder();
		manager.setFields(new String[]{"20120801121212", "123\003456\003789\002a\003b\003c\002q\003w\003r"}, new String[]{"\001","\002", "\003"});
		manager.doFieldMapping("tt_flow_spout", builder);
		Assert.assertEquals(builder.getMid(),"789\001c\001r");
		
	}
	@Test
	public void test2() throws LogFormatException, UnsupportedEncodingException {
		LogMapping logMappings = new LogMapping();
		List<LogMappingItem> mappings = new ArrayList<LogMappingItem>();
		logMappings.put("collect_info", mappings);
		mappings.add(new LogMappingItem("delete_time", "@field2 5 38"));
		mappings.add(new LogMappingItem("item_id", "@bigint @field2 5 32"));
		
		FieldMappingManager manager = new FieldMappingManager("business", logMappings );
		BusinessStarLog.Builder builder = BusinessStarLog.newBuilder();
		String s="1349848660851035:95452663\002favorite:collect_info\002UPDATE\0020:000000000AAFBC26010000000154CCE40E\002\001179289126\0010\0012012-10-10 13:51:33\001\0010\001\0012012-10-10 13:51:33\0012012-10-10 13:51:33\001xuqian868\001\0011\001\001\0010\001\001\001\001\001\001\001\001\001\001\001\001\001\001\001\001\0015717681166\0010\0012012-10-10 13:51:33\0013\001151\001\001\001";
		String[] ss = Tools.split(s, "\002");
		manager.setFields(ss, new String[]{"\002","\001"});
		manager.doFieldMapping("collect_info", builder);
		Assert.assertEquals(builder.getItemId(), 5717681166L);
		Assert.assertEquals(builder.getDeleteTime(), "");
	}
}
