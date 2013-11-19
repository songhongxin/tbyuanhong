package com.etao.lz.star;

import org.junit.Test;

public class LocalHBaseFlowTest extends LocalHBaseTestBase {
	public String[] getTables() {
		return new String[] {"browse_log"};
	}
	public String[] getFiles(String id) {
		return new String[] {"browse_log-"+id+".hbase"};
	}
	@Test
	public void testBizorder() throws Exception {
		doTest("local_flow");
	}
	@Override
	public String getLogType() {
		return "flow";
	}
	@Override
	public String[] getIndexTables() {
		return null;
	}
}
