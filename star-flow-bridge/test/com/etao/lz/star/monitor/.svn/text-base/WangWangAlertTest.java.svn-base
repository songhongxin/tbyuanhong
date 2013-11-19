package com.etao.lz.star.monitor;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

public class WangWangAlertTest {
	private List<String> urls =new ArrayList<String>();
	@Test
	public void test() {
		WangWangAlert alter = new WangWangAlert("国相,太奇"){
			@Override
			void open(String u) {
				urls.add(u);
			}
		};
		alter.alert("测试", "测试消息");
		Assert.assertEquals(urls.size(), 2);
		Assert.assertEquals(urls.get(0), "http://10.246.155.184:9999/wwnotify.war/mesg?user=%B9%FA%CF%E0&subject=%B2%E2%CA%D4&msg=%B2%E2%CA%D4%CF%FB%CF%A2");
		Assert.assertEquals(urls.get(1), "http://10.246.155.184:9999/wwnotify.war/mesg?user=%CC%AB%C6%E6&subject=%B2%E2%CA%D4&msg=%B2%E2%CA%D4%CF%FB%CF%A2");
	}

}
