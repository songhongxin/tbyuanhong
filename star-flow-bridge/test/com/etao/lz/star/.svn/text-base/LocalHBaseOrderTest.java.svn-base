package com.etao.lz.star;

import org.junit.Test;

public class LocalHBaseOrderTest extends LocalHBaseTestBase {
	public String[] getTables() {
		return new String[] { "alipay_rtn_order" };
	}

	public String[] getFiles(String id) {
		return new String[] { "alipay_rtn_order-" + id + ".hbase" };
	}

	@Test
	public void testBizorder() throws Exception {
		doTest("local_order");
	}

	@Override
	public String getLogType() {
		return "business";
	}

	@Override
	public String[] getIndexTables() {
		return null;
	}
}
