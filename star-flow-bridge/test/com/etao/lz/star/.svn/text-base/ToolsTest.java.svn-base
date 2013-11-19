package com.etao.lz.star;

import java.io.ByteArrayInputStream;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

public class ToolsTest {

	@Test
	public void testGetDescriptor() {
		Assert.assertEquals(Tools.getDescriptor("flow"),StarLogProtos.FlowStarLog.getDescriptor());
		Assert.assertEquals(Tools.getDescriptor("business"),StarLogProtos.BusinessStarLog.getDescriptor());
	}
	
	@Test
	public void testFormat()
	{
		long time = Tools.parseSimpleTime("20120626045043");
		Assert.assertEquals(Tools.formatTime(time),"2012-06-26 04:50:43");
	}
	
	@Test
	public void testSplit10()
	{
		String s="1349848660851035:95452663\002favorite:collect_info\002UPDATE\0020:000000000AAFBC26010000000154CCE40E\002\001179289126\0010\0012012-10-10 13:51:33\001\0010\001\0012012-10-10 13:51:33\0012012-10-10 13:51:33\001xuqian868\001\0011\001\001\0010\001\001\001\001\001\001\001\001\001\001\001\001\001\001\001\001\0015717681166\0010\0012012-10-10 13:51:33\0013\001151\001\001\001";
		String[] ss = Tools.split(s, "\002");
		Assert.assertEquals(ss.length, 5);
		ss = Tools.split(ss[4], "\001");
		Assert.assertEquals(ss.length, 39);
		System.out.println(ss[31]);
	}
	@Test
	public void testSplit4()
	{
		String s = "a\"\"\"";
		String[] split = Tools.split(s, "\"");
		Assert.assertEquals(split.length, 4);
		Assert.assertEquals(split[0], "a");
		Assert.assertEquals(split[1], "");
		Assert.assertEquals(split[2], "");
		Assert.assertEquals(split[3], "");
	}
	
	@Test
	public void testSplit3()
	{
		String s = "aaa\\\"aa\001aa";
		String[] split = Tools.split(s, "\001");
		Assert.assertEquals(split.length, 2);
		Assert.assertEquals(split[0], "aaa\\\"aa");
		Assert.assertEquals(split[1], "aa");
	}
	@Test
	public void testSplit()
	{
		byte[] a= {0x1, 0x2, 0x3, 0, 0x6,0x5,0x4};
		List<byte[]> split = Tools.split(a, (byte)0);
		byte[] a1 = split.get(0);
		byte[] a2 = split.get(1);
		Assert.assertEquals(a1.length,3);
		Assert.assertEquals(a2.length,3);
		Assert.assertEquals(a1[0], 0x1);
		Assert.assertEquals(a1[1], 0x2);
		Assert.assertEquals(a1[2], 0x3);
		Assert.assertEquals(a2[0], 0x6);
		Assert.assertEquals(a2[1], 0x5);
		Assert.assertEquals(a2[2], 0x4);
	}
	
	@Test
	public void testPort()
	{
		String line = "tcp        0      0 0.0.0.0:6707                0.0.0.0:*                   LISTEN      30046/java";
		Assert.assertEquals(Tools.getListenPort(new ByteArrayInputStream(line.getBytes()),"30046"),"6707");
		Assert.assertEquals(Tools.getListenPort(new ByteArrayInputStream(line.getBytes()), "30045"),"Unknown");
	}

	@Test
	public void testByteSplit()
	{
		byte[] a = {0,1,2};
		byte[] b = {5,4,3,0,1,2,3,4,5,0,1,2,7,8,9};
		List<byte[]> split = Tools.split(b, a);
		Assert.assertEquals(split.size(), 3);
		Assert.assertEquals(split.get(0).length, 3);
		Assert.assertEquals(split.get(1).length, 3);
		Assert.assertEquals(split.get(2).length, 3);
		
		int offset[] ={0,6,12};
		for(int i=0;i<split.size();++i)
		{
			for(int j=0;j<split.get(i).length;++j)
			{
				Assert.assertEquals(split.get(i)[j], b[offset[i] + j]);
			}
		}
		
	}
}
