package com.etao.lz.storm.detailcalc;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.etao.lz.storm.utils.MiscTestUtil;


public class AlizhgDetailTest {
	
	private static final String PRX = "storm/qa-2013-11-11/";
	
	@Before
	public void tearDown() {
		
	}

	@After
	public void setUp() {
		
	}

	
	@Test
	public void testCase1() throws Exception {
		drive(1);
	}

	
	void drive(int n) throws Exception {
		MiscTestUtil.zhgdetailCalcDriver(
				new AlizhgDetail(),
				PRX + n + "/browse.data",
				PRX + n + "/biz.data",
				PRX + n + "/zresult.yml",
				null);
	}


}
