package com.etao.lz.star;

import junit.framework.Assert;

import org.junit.Test;

public class StarConfigTest {

	@Test
	public void test() throws Exception {
		StarConfig conf = new StarConfig("test");
		XMLParser parser = new XMLParser("/test_common.xml");
		conf.parseCommon(parser);
		Assert.assertEquals("1", conf.get("spout_number"));
		Assert.assertEquals("8", conf.get("filling_number"));
		Assert.assertEquals("8", conf.get("rulling_number"));
		Assert.assertEquals("8", conf.get("business_number"));
		Assert.assertEquals(8, conf.getInt("writer_number"));
		Assert.assertEquals("a && b", conf.get("script").trim());
		Assert.assertTrue(conf.getBool("bool_test", true));
		Assert.assertTrue(conf.getBool("no_exists", true));
		Assert.assertFalse(conf.getBool("no_exists", false));
	}
	
	@Test
	public void testInherit() throws Exception {
		StarConfig conf = new StarConfig("test");
		XMLParser parser = new XMLParser("/test_common.xml");
		conf.parseCommon(parser);
		conf = new StarConfig("test", conf);
		Assert.assertEquals("1", conf.get("spout_number"));
		Assert.assertEquals("8", conf.get("filling_number"));
		Assert.assertEquals("8", conf.get("rulling_number"));
		Assert.assertEquals("8", conf.get("business_number"));
		Assert.assertEquals(8, conf.getInt("writer_number"));
		Assert.assertTrue(conf.getBool("bool_test", true));
		Assert.assertTrue(conf.getBool("no_exists", true));
		Assert.assertFalse(conf.getBool("no_exists", false));
	}
}
