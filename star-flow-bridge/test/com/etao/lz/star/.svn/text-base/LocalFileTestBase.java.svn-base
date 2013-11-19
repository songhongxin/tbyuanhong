package com.etao.lz.star;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public abstract class LocalFileTestBase {

	private static Log LOG = LogFactory.getLog(LocalFileTestBase.class);
	
	public static void diffFile(String expect, String result) throws Exception {
		File f = new File(result);
		long time = f.lastModified();
		while (true) {
			Thread.sleep(10000);
			if (f.length() == 0)
			{
				LOG.info("retry file time check:"+result);
				continue;
			}
			long now = f.lastModified();
			if (time == now)
				break;
			time = now;
			LOG.info("retry file time check:appending is still going on");
		}
		BufferedReader expect_reader = new BufferedReader(
				new FileReader(expect));
		BufferedReader result_reader = new BufferedReader(
				new FileReader(result));
		while (true) {
			String expect_line = expect_reader.readLine();
			String result_line = result_reader.readLine();
			if (expect_line == null) {
				Assert.assertNull(result_line);
				break;
			}
			Assert.assertEquals(result, expect_line, result_line);
		}
		expect_reader.close();
		result_reader.close();
	}

	@Before
	public void setUp() throws Exception {
	}


	@After
	public void tearDown() throws Exception {

	}

	public void doTest(String base) throws Exception {

		LocalCluster cluster = new LocalCluster();
		StarMain main = new StarMain(base, base + "-topo");

		TopologyBuilder stormBuilder = new TopologyBuilder();
		main.config(stormBuilder);
		Config conf = new Config();
		conf.setNumAckers(0);
		conf.setNumWorkers(5);

		cluster.submitTopology(base + "-topo", conf,
				stormBuilder.createTopology());
		
		main.createZKDir();
		
		String file = getFile();
		LOG.info("check file result:"+ file);
		diffFile(String.format("data/%s", file), String.format("/tmp/%s", file));
		cluster.shutdown();
	}

	public abstract String getFile();

}
