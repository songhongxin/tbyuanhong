package com.etao.lz.star.output.hbase;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

import com.etao.lz.star.Tools;
import com.google.protobuf.GeneratedMessage;

public class FileHelper implements OutputHelper {
	private OutputStreamWriter writer;
	public FileHelper(String path) throws IOException
	{
		writer = new OutputStreamWriter(new FileOutputStream(path,false),"utf-8");
	}
	
	@Override
	public void flush() throws IOException {
		writer.flush();
	}

	@Override
	public void write(byte[] rowKey, byte[] data, GeneratedMessage log) throws IOException {
		writer.write("RowKey:");
		writer.write(Tools.bytesToString(rowKey));
		writer.write("\nData:");
		writer.write(Tools.bytesToString(data));
		writer.write("\n");
		if(log != null)
		{
			writer.write(log.toString());
			writer.write("\n");
		}
	}
}
