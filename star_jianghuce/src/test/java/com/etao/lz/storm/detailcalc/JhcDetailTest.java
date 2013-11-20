package com.etao.lz.storm.detailcalc;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.etao.lz.storm.utils.MiscTestUtil;

public class JhcDetailTest {

	private static final String PRX = "storm/qa-2013-11-11/";

	@Before
	public void tearDown() {

	}

	@After
	public void setUp() {

	}

	
	@Test
	public void testCase1() throws Exception {
		drive(2);
	}
	
	@Test
	public void testCase2() throws Exception {
		drive(3);
	}
	
	void drive(int n) throws Exception {
		MiscTestUtil.jhcdetailCalcDriver(new JhcDetail(false), PRX + n
				+ "/browse.data", PRX + n + "/biz.data", PRX + n
				+ "/result.yml", null);
	}

}
