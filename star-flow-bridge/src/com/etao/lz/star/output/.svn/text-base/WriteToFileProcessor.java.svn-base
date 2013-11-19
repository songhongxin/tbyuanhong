package com.etao.lz.star.output;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.etao.lz.star.StarConfig;
import com.etao.lz.star.Tools;
import com.google.protobuf.GeneratedMessage;

public class WriteToFileProcessor implements LogOutput {
	private static final long serialVersionUID = 6088649724604137974L;
	private transient OutputStreamWriter writer;
	private String path;
	private Log LOG = LogFactory.getLog(WriteToFileProcessor.class);

	@Override
	public void config(StarConfig config) {
		path = config.get("write_path");
		LOG.info("WriteToFileProcessor:" + path);
	}

	@Override
	public void output(byte[] data, GeneratedMessage msg) {
		try {
			String s = msg.toString();
			writer.write(s);
			writer.write("\n");
			writer.flush();
		} catch (IOException e) {
			LOG.error(Tools.formatError("WriteToFileOutput Error", e));
		}
	}

	@Override
	public void open(int id) {
		try {
			writer = new OutputStreamWriter(new FileOutputStream(String.format(path, id), false), "utf-8");
		} catch (IOException e) {
			throw new IllegalStateException(e);
		}
	}

	@Override
	public void getStat(Map<String, String> map) {
		
	}

}
