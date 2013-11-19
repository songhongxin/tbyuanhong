package com.etao.lz.star.monitor;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLEncoder;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.etao.lz.star.Tools;

public class WangWangAlert {
	private String users[];
	private static Log LOG = LogFactory.getLog(WangWangAlert.class);
	
	public WangWangAlert(String userList) {
		users = userList.split(",");
	}

	public void alert(String subject, String content) {
		try {
			subject = URLEncoder.encode(subject, "gbk");
			content = URLEncoder.encode(content, "gbk");
		} catch (UnsupportedEncodingException e) {
			throw new IllegalStateException(e);
		}

		for (String user : users) {
			try {
				user = URLEncoder.encode(user.trim(), "gbk");
			} catch (UnsupportedEncodingException e) {
				throw new IllegalStateException(e);
			}
			String u = String
					.format("http://10.246.155.184:9999/wwnotify.war/mesg?user=%s&subject=%s&msg=%s",
							user, subject, content);
			open(u);
		}
	}

	void open(String u) {
		LOG.info(u);
		try {
			URL url = new URL(u);
			URLConnection conn = url.openConnection();
			HttpURLConnection httpConn = (HttpURLConnection) conn;
			httpConn.setUseCaches(false);
			InputStream in = httpConn.getInputStream();
			BufferedReader r = new BufferedReader(new InputStreamReader(in, "utf-8"));
			String line = null;
			while ((line = r.readLine()) != null) {
				LOG.info(line);
			}
		} catch (Exception e) {
			LOG.error(Tools.formatError("WangWangAlert", e));
			return;
		}
	}

}
