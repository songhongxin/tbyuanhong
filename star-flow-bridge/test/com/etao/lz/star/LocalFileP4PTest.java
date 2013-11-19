package com.etao.lz.star;

import org.junit.Test;

public class LocalFileP4PTest extends LocalFileTestBase {

	public String getFile() {
		return "p4p.data";
	}

	@Test
	public void test() throws Exception {
		doTest("local_p4p");
	}
}
