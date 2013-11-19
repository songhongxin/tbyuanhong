package com.etao.lz.star;

import org.junit.Test;

public class LocalFileClickTest extends LocalFileTestBase {

	public String getFile() {
		return "click.data";
	}

	@Test
	public void test() throws Exception {
		doTest("local_click");
	}
}
