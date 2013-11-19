package com.etao.lz.star.output.hbase;

import java.io.IOException;
import java.io.Serializable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import com.etao.lz.star.StarConfig;
import com.etao.lz.star.Tools;
import com.google.common.base.Preconditions;
import com.google.protobuf.GeneratedMessage;

public class LogWriter implements Serializable {

	private Log LOG = LogFactory.getLog(LogWriter.class);

	private static final long serialVersionUID = -1464866151544454706L;

	private transient RowKeyBuilder rowkeyBuilder;

	private String tableName;
	private String indexName;

	private String destType;

	private StarConfig _config;

	private transient OutputHelper _table;
	private transient OutputHelper _indexTable;
	private String rowkeyClass;
	public LogWriter(StarConfig config, String tableName) {
		this._config = config;
		this.tableName = tableName;
		String[] defines = config.get(tableName + "_input").split(",");
		if (defines.length < 2) {
			throw new IllegalStateException(String.format(
					"Wrong definition for %s", tableName));
		}
		this.indexName = defines.length >= 3 ? defines[2] : null;

		destType = defines[0].trim();
		rowkeyClass = defines[1].trim();
	}

	public void open(int taskId) {
		try {
			try {
				rowkeyBuilder = (RowKeyBuilder) Class.forName(rowkeyClass)
						.newInstance();
			} catch (Exception e) {
				throw new IllegalStateException(e);
			}
			String fileBase = _config.get("output_file_base", null);
			if (fileBase == null) {
				Configuration conf = HBaseConfiguration.create();
				HBaseAdminHelper.doHBaseConfig(conf, _config.get("Zookeepercluster"),
						_config.get("HbasePort"), null);
				long writeBufferSize = Long.parseLong(_config.get("writeBufferSize"));
				_table = new HBaseTableHelper(tableName, conf, writeBufferSize);
				if (indexName != null) {
					_indexTable = new HBaseTableHelper(indexName, conf,	writeBufferSize);
				}
			} else {
				_table = new FileHelper(String.format("%s/%s-%d.hbase",
						fileBase, tableName, taskId));
				if (indexName != null) {
					_indexTable = new FileHelper(String.format(
							"%s/%s-%d.hbase", fileBase, indexName, taskId));
				}
			}
		} catch (IOException e) {
			throw new IllegalStateException(e);
		}

	}

	public String getType() {
		return destType;
	}

	private void write(GeneratedMessage minLog, byte[] data, int taskId) {
		if (minLog == null)
			return;
		byte[] rowkey = rowkeyBuilder.build(minLog, (short) taskId);
		if (rowkey == null) {
			return;
		}

		try {
			_table.write(rowkey, data, minLog);
		} catch (IOException e) {
			LOG.error(Tools.formatError("OutputLogQueue Error", e));
		}

		if (_indexTable == null)
			return;
		byte[] indexRowkey = rowkeyBuilder.buildIndex(minLog, (short) taskId);
		if (indexRowkey == null) {
			return;
		}
		try {
			_indexTable.write(indexRowkey, rowkey, null);
		} catch (IOException e) {
			LOG.error(Tools.formatError("OutputLogQueue Error", e));
		}
	}

	public synchronized void flush() {
		Preconditions.checkNotNull(_table);
		try {
			_table.flush();
		} catch (IOException e) {
			LOG.error(Tools.formatError("OutputLogQueue Error ", e));
		}

		if (_indexTable != null) {
			try {
				_indexTable.flush();
			} catch (IOException e) {
				LOG.error(Tools.formatError("OutputLogQueue Error", e));
			}
		}
	}

	public synchronized void add(byte[] data, GeneratedMessage log, int taskId) {
		write(log, data, taskId);
	}

}
