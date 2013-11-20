package com.etao.lz.storm.tools;


/**
 * 细水绕田园相关常量
 * 
 * @author yuanhong.shx
 * 
 */
public interface Constants {

	// mysql test配置
	/*
	 * public static final String MYSQLHOST =
	 * "jdbc:mysql://10.235.160.137:3306/fmp"; public static final String
	 * MYSQLUSER = "lzstat"; public static final String MYSQLPASSWORD =
	 * "711TJS";
	 */
	// mysql online配置

	public static final String MYSQLHOST = "jdbc:mysql://172.24.65.65:3306/fmp";
	public static final String MYSQLUSER = "fmpuser";
	public static final String MYSQLPASSWORD = "Hqke78hfgHqwn";

	// HBase 表中的分区数量
	public static final int BIGB_HBASE_SHARDING_NUM = 256;

	// HBase 表中的时间周期
	public static final int BIGB_HBASE_TIME_INTERVAL = 4 * 24 * 3600;

	// HBase 表名
	public static final String DP_H_TABLE = "lz_st_shop_item_h_test";
	public static final String DP_D_TABLE = "lz_st_shop_item_d_test";

	// 流量日志流名称
	public static final String TRAFFIC_STREAMID = "T";
	// 业务日志流名称
	public static final String BIZ_STREAMID = "B";

	// mysql刷新间隔
	public int MYSQL_FLUSH_TT = 3;
	// HBase刷新间隔
	public int HBASE_FLUSH_TT = 1;
	// hbase刷新队列
	public int HBASE_QUEUE_MAX = 1000;
	// mysql刷新队列
	public int MYSQL_QUEUE_MAX = 1500;
	// HBase刷新调整间隔
	public int HBASE_FLUSH_NEW_TT = 3;
	// HBase: storemap size max
	public int STOREMAP_MAX_SIZE = 40;

	public int STOREMAP_MIN_SIZE = 20;

	public long ALIPAYAMT_EVENT_BASE = 1000000000;

	// 吸星大法 HBase 保存 ProtoBuffer 序列化内容的列名
	public static final String XXDF_HBASE_PB_COL = "pb";
	public static final String XXDF_HBASE_FAMILY = "d";

	// HBase family 名
	public static final String BIGB_HBASE_FAMILY = "d";

	public static final String DETAIL_BOLT_ID = "detailBolt";

	/*
	 * //吸星大法虫洞表 public static final String XXDF_HBASE_BIZORDER_TABLE =
	 * "biz_order"; public static final String XXDF_HBASE_PAYORDER_TABLE =
	 * "pay_order"; public static final String XXDF_HBASE_BIZORDER_IDX_TABLE =
	 * "biz_order_index"; public static final String
	 * XXDF_HBASE_PAYORDER_IDX_TABLE = "pay_order_index";
	 * 
	 * 
	 * 
	 * 
	 * // 水绕田园HBase表名 public static final String[] HBASE_TABLE_NAMES = new
	 * String[] { "bigb-bu-dp-t", "bigb-bu-dp-b", "bigb-flow-dp-t",
	 * "bigb-flow-dp-xb" };
	 * 
	 * 
	 * 
	 * 
	 * 
	 * // 吸星大法HBase 表读取Spout每次读取到多长时间前的数据，单位为秒，默认为3分钟前 public static final int
	 * XXDF_HBASE_LOG_SCAN_END_SEC = 3 * 60; // 吸星大法HBase
	 * 表读取Spout每次最多读取多长时间前的数据，单位为秒，默认为3分钟 public static final int
	 * XXDF_HBASE_LOG_SCAN_MAX_SECS = 3 * 60; // 吸星大法HBase 表读取Spout客户端缓存的主表的记录条数
	 * public static final int XXDF_HBASE_MAIN_TABLE_RECORD_NUM = 500; //
	 * 吸星大法HBase 表读取Spout客户端缓存的索引表的记录条数 public static final int
	 * XXDF_HBASE_INDEX_TABLE_RECORD_NUM = 20;
	 * 
	 * 
	 * 
	 * 
	 * // 业务日志流名称 public static final String BIZ_STREAMID = "B";
	 * 
	 * 
	 * // 业务Spout的componetID public static final String BUSINESS_SPOUT_ID =
	 * "businessSpout"; // 详细指标计算Bolt的componetID public static final String
	 * DETAIL_BOLT_ID = "detailBolt"; // 详细指标计算Bolt的componetID public static
	 * final String REDUCE_BOLT_ID = "reduceBolt";
	 * 
	 * // ZooKeeper 配置键 public static final String ZK_SERVERS = "zk.servers"; //
	 * 连接字符串 public static final String ZK_RETRY_TIMES = "zk.retry_times"; //
	 * 连接重试次数 public static final String ZK_RETRY_SLEEP_INTERVAL =
	 * "zk.retry_sleep_interval"; // 连接重试中sleep时长
	 */
	// UV 去重位图长度(以2的指数表示)
	public static final int UV_BITMAP_K = 13;

	// HBase 写缓冲区大小(in bytes)
	public static final int HBASE_WBUF_SIZE = 65536 * 1024;

}
