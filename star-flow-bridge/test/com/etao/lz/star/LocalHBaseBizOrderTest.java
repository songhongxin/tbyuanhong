package com.etao.lz.star;

import org.junit.Test;





public class LocalHBaseBizOrderTest extends LocalHBaseTestBase {
	public String[] getTables() {
		return new String[] {"biz_order"};
	}
	public String[] getFiles(String id) {
		return new String[] {"biz_order-"+id+".hbase","biz_order_index-"+id+".hbase"};
	}
	@Test
	public void testBizorder() throws Exception {
		doTest("local_bizorder");
	}
	@Override
	public String getLogType() {
		return "business";
	}
	@Override
	public String[] getIndexTables() {
		return new String[] {"biz_order_index"};
	}
}
