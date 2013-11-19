package com.etao.lz.star;

import java.io.IOException;

import com.etao.lz.star.monitor.TTMonitor;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.retry.RetryNTimes;

public class ZKTools {
	public static CuratorFramework startCuratorClient(String zookeeperCluster) {
		try {
			CuratorFramework client = CuratorFrameworkFactory.builder()
					.connectString(zookeeperCluster)
					.namespace(TTMonitor.ZK_NAMESPACE)
					.retryPolicy(new RetryNTimes(Integer.MAX_VALUE, 1000))
					.connectionTimeoutMs(5000).build();
			client.start();
			return client;
		} catch (IOException e) {
			throw new IllegalStateException(e);
		}
	}

}
