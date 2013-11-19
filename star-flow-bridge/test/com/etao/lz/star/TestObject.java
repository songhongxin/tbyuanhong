package com.etao.lz.star;

import com.etao.lz.star.spout.LogMapping;

class TestObject {
	private String msg;
	private int no;
	private boolean boolTest;
	private TestObject test;
	private LogMapping mapping;
	public LogMapping getMapping() {
		return mapping;
	}
	public void setMapping(LogMapping mapping) {
		this.mapping = mapping;
	}
	public void setMsg(String msg) {
		this.msg = msg;
	}
	public void setNo(int no) {
		this.no = no;
	}
	public void setBoolTest(boolean boolTest) {
		this.boolTest = boolTest;
	}
	public void setTest(TestObject t)
	{
		test = t;
	}

	public String getMsg() {
		return msg;
	}
	
	public int getNo() {
		return no;
	}
	
	public boolean isBoolTest() {
		return boolTest;
	}
	public TestObject getTest() {
		return test;
	}
}
