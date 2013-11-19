package com.etao.lz.star;

import org.junit.Test;





public class LocalHBasePayOrderTest extends LocalHBaseTestBase {
	public String[] getTables() {
		return new String[] {"pay_order"};
	}
	public String[] getFiles(String id) {
		return new String[] {"pay_order-"+id+".hbase", "pay_order_index-"+id+".hbase"};
	}
	@Test
	public void testBizorder() throws Exception {
		doTest("local_payorder");
	}
	@Override
	public String getLogType() {
		return "business";
	}
	@Override
	public String[] getIndexTables() {
		return new String[] {"pay_order_index"};
	}

}
