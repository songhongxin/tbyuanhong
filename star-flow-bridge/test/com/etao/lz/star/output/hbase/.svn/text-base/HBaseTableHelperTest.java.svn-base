package com.etao.lz.star.output.hbase;

import junit.framework.Assert;

import org.junit.Test;

public class HBaseTableHelperTest {

	@Test
	public void testAdjustEnd() {
		int n = HBaseTableHelper.adjustEnd(1000, 12345);
		Assert.assertEquals(n,  12345);
		
		n = HBaseTableHelper.adjustEnd(1000, HBaseAdminHelper.WEEK_DAYS * HBaseAdminHelper.DAY_HOURS
				* HBaseAdminHelper.HOUR_SECONDS + 123);
		Assert.assertEquals(n,  HBaseAdminHelper.WEEK_DAYS * HBaseAdminHelper.DAY_HOURS
				* HBaseAdminHelper.HOUR_SECONDS);
		n = HBaseTableHelper.adjustEnd(1000, HBaseAdminHelper.WEEK_DAYS * HBaseAdminHelper.DAY_HOURS
				* HBaseAdminHelper.HOUR_SECONDS);
		Assert.assertEquals(n,  HBaseAdminHelper.WEEK_DAYS * HBaseAdminHelper.DAY_HOURS
				* HBaseAdminHelper.HOUR_SECONDS);
	}

}
