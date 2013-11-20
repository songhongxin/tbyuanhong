package com.etao.lz.storm;

import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropConfig {

	private Properties props = null;

	public PropConfig() {
		props = new Properties();
	}

	public PropConfig(String path) throws IOException {
		props = new Properties();
		loadResource(path);
	}

	public void loadResource(String path) throws IOException {

		if (!path.startsWith("/")) {
			// 相对路径作为系统资源对待
			InputStream in = ClassLoader.getSystemResourceAsStream(path);
			props.load(in);
			in.close();
		} else {
			FileReader fr = new FileReader(path);
			props.load(fr);
			fr.close();
		}
		
	}

	public String getProperty(String key) {
		return props.getProperty(key);
	}
	
	/*
	public  static  void  main(String [] args) throws IOException{
		
		
		PropConfig pc = new PropConfig("storm/bolt-config.properties");
		
		System.out.println(pc.getProperty("zk.servers"));
		
	}
	*/
}
