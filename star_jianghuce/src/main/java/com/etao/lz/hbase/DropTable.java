package com.etao.lz.hbase;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class DropTable {
	
	private static final Log LOG = LogFactory.getLog(DropTable.class);

//	private static final Configuration conf = HBaseConfiguration.create();

	private HBaseAdmin admin;

	public DropTable(HBaseAdmin admin) {
		this.admin = admin;
	}

	/**
	 * 删除HBase表
	 * 
	 * @param tableName
	 * @return
	 */
	public boolean doWork(String tableName) {
		try {
			if (admin.tableExists(tableName)) {
				admin.disableTable(tableName);
				admin.deleteTable(tableName);
				LOG.info("table: " + tableName
						+ " has been disable and deleted");
			} else {
				LOG.error("table: " + tableName
						+ " not exists, failed to delete");
			}
		} catch (IOException e) {
			e.printStackTrace();
			LOG.error(e);
			return false;
		}
		return true;
	}

	/**
	 * 删除水绕田园HBase表
	 * 
	 * @param args
	 
	public static void main(String args[]) {
		HBaseAdmin admin = null;
		try {
			admin = new HBaseAdmin(conf);
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
			LOG.error(e);
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
			LOG.error(e);
		}
		DropTable dropTable = new DropTable(admin);
		String[] tableNames = null;
		if (args.length > 0)
			tableNames = args;
		else
			tableNames = Constants.HBASE_TABLE_NAMES;
		for (String tableName : tableNames) {
			dropTable.doWork(tableName);
		}
	}
*/
}
