package com.etao.lz.star;

import org.junit.Test;

public class LocalFileCollectInfoTest extends LocalFileTestBase {

	public String getFile() {
		return "collect.data";
	}

	@Test
	public void test() throws Exception {
		doTest("local_collect_info");
	}
}
