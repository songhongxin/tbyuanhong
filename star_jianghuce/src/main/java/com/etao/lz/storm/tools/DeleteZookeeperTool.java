package com.etao.lz.storm.tools;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.RetryNTimes;

/*
 * 
 * 用于测试的时候，删除zk中的临时状态
 * */

public class DeleteZookeeperTool {

	// Zookeeper客户端
	static CuratorFramework zkClient;
	public static final String zkQuorum = "newst2.lzs.cm4,newst3.lzs.cm4,newst4.lzs.cm4";
	static int zkRetryTimes = 10;
	static int zkRetrySleepInterval = 20;
	static String path = "/jhc/pem/sync/";

	// 日志操作记录对象
	private static final Log LOG = LogFactory.getLog(DeleteZookeeperTool.class);

	/**
	 * 启动Zookeeper客户端连接
	 */
	public static void startZookeeperClient() {
		try {
			zkClient = CuratorFrameworkFactory
					.builder()
					.connectString(zkQuorum)
					.retryPolicy(
							new RetryNTimes(zkRetryTimes, zkRetrySleepInterval))
					.build();
			zkClient.start();
		} catch (IOException e1) {
			LOG.error("failed to connect to zookeeper", e1);
		}
	}

	public static void main(String[] args) throws Exception {

		startZookeeperClient();

		for (int x = 0; x < 33; x++) {

			String depath = path + x;
			zkClient.delete().forPath(depath);

		}

	}

}
