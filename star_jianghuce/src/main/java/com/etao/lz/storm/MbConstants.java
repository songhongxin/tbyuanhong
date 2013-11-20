package com.etao.lz.storm;

import java.util.regex.Pattern;

/**
 * 效果计算平台相关常量
 * 
 * @author wxz
 * 
 */
public interface MbConstants {

	// 吸星大法 HBase 表中的分区数量
	public static final int XXDF_HBASE_SHARDING_NUM = 32;

	// 吸星大法 HBase 表中的时间周期
	public static final int XXDF_HBASE_TIME_INTERVAL = 7 * 24 * 3600;

	// 吸星大法 HBase 表名
	public static final String XXDF_HBASE_FLOW_TABLE = "browse_log";
	public static final String XXDF_HBASE_ADCLK_TABLE = "ad_ex_click";
	public static final String XXDF_HBASE_BIZORDER_TABLE = "biz_order";
	public static final String XXDF_HBASE_PAYORDER_TABLE = "pay_order";
	public static final String XXDF_HBASE_BIZORDER_IDX_TABLE = "biz_order_index";
	public static final String XXDF_HBASE_PAYORDER_IDX_TABLE = "pay_order_index";
	public static final String XXDF_HBASE_ALIPAY_RTNORDER_TABLE = "alipay_rtn_order";

	// 吸星大法 HBase family 名
	public static final String XXDF_HBASE_FAMILY = "d";

	// 吸星大法 HBase 保存 ProtoBuffer 序列化内容的列名
	public static final String XXDF_HBASE_PB_COL = "pb";

	// 吸星大法HBase 表读取Spout每次读取到多长时间前的数据，单位为秒，默认为3分钟前
	public static final int XXDF_HBASE_LOG_SCAN_END_SEC = 3 * 60;
	// 吸星大法HBase 表读取Spout每次最多读取多长时间前的数据，单位为秒，默认为3分钟
	public static final int XXDF_HBASE_LOG_SCAN_MAX_SECS = 3 * 60; // 当日志源发生故障时/或者流量极少的情况，修改次数的时间，调大重启即可
	// 吸星大法HBase 表读取Spout客户端缓存的主表的记录条数
	public static final int XXDF_HBASE_MAIN_TABLE_RECORD_NUM = 500;
	// 吸星大法HBase 表读取Spout客户端缓存的索引表的记录条数
	public static final int XXDF_HBASE_INDEX_TABLE_RECORD_NUM = 20;

	// 吸星大法HBase 表读取Spout每次读最多读取到多长时间前的数据，单位为秒，默认为1分钟前，防止跟的太紧丢失数据
	public static final int XXDF_HBASE_LOG_SCAN_NOW_SEC = 1 * 60;

	// 月光宝盒PEM版本报表HBase表名前缀
	public static final String YGBH_PEM_HBASE_GROUND_TABLE = "pem_rpt_grd";
	public static final String YGBH_PEM_HBASE_POS_TABLE = "pem_rpt_pos";
	public static final String YGBH_PEM_HBASE_POS_DESC_TABLE = "pem_rpt_pos_desc";
	public static final String YGBH_PEM_HBASE_SHOP_TABLE = "pem_rpt_shop";

	// 月光宝盒 HBase family 名
	public static final String YGBH_HBASE_FAMILY = "d";

	// Storm 自定义参数配置文件资源路径
	public static final String STORM_CONF_TASK_PROPERTY_PATH = "task_prop_path";

	// Storm 自定义参数卖家shopid->sellerid映射资源路径
	public static final String STORM_CONF_SHOP_SELLERID_PATH = "shop_sellerid_path";

	// 流量日志流名称
	public static final String TRAFFIC_STREAMID = "T";
	// 业务日志流名称
	public static final String BIZ_STREAMID = "B";

	// 流量Spout的componentID
	public static final String TRAFFIC_SPOUT_ID = "trafficSpout";
	// 业务Spout的componetID
	public static final String BUSINESS_SPOUT_ID = "businessSpout";
	// 详细指标计算Bolt的componetID
	public static final String DETAIL_BOLT_ID = "detailBolt";
	// 详细指标计算Bolt的componetID
	public static final String REDUCE_BOLT_ID = "reduceBolt";

	// ZooKeeper 配置键
	public static final String ZK_SERVERS = "zk.servers"; // 连接字符串
	public static final String ZK_RETRY_TIMES = "zk.retry_times"; // 连接重试次数
	public static final String ZK_RETRY_SLEEP_INTERVAL = "zk.retry_sleep_interval"; // 连接重试中sleep时长

	// TrafficSpout 配置键
	public static final String TRAFFIC_SPOUT_TASKS = "traffic_spout.tasks";
	public static final String TRAFFIC_SPOUT_QUEUE_SIZE = "traffic_spout.queue_size";
	public static final String TRAFFIC_SPOUT_ENABLE_COMPRESS = "traffic_spout.enable_compress";
	public static final String TRAFFIC_SPOUT_OUTPUT_MODE_PER_SHARD = "traffic_spout.output_mode_per_shard";
	public static final String TRAFFIC_SPOUT_OUTPUT_MODE_PER_SPOUT = "traffic_spout.output_mode_per_spout";
	public static final String TRAFFIC_SPOUT_SYNC_TS_INTERVAL = "traffic_spout.sync_ts_interval";
	public static final String TRAFFIC_SPOUT_START_TIMESTAMP = "traffic_spout.start_timestamp";
	public static final String TRAFFIC_SPOUT_END_TIMESTAMP = "traffic_spout.end_timestamp";

	// BusinessSpout 配置键
	public static final String BUSINESS_SPOUT_TASKS = "business_spout.tasks";
	public static final String BUSINESS_SPOUT_QUEUE_SIZE = "business_spout.queue_size";
	public static final String BUSINESS_SPOUT_START_TIMESTAMP = "business_spout.start_timestamp";
	public static final String BUSINESS_SPOUT_END_TIMESTAMP = "business_spout.end_timestamp";
	public static final String BUSINESS_SPOUT_COUNT_DATE = "business_spout.count_date";
	public static final String BUSINESS_SPOUT_ENABLE_COMPRESS = "business_spout.enable_compress";
	public static final String BUSINESS_SPOUT_OUTPUT_MODE_PER_SHARD = "business_spout.output_mode_per_shard";
	public static final String BUSINESS_SPOUT_OUTPUT_MODE_PER_SPOUT = "business_spout.output_mode_per_spout";
	public static final String BUSINESS_SPOUT_SYNC_TS_INTERVAL = "business_spout.sync_ts_interval";
	public static final String BUSINESS_SPOUT_SYNC_TS_LATENCY = "business_spout.sync_ts_latency";

	// DetailBolt 配置键
	public static final String DETAIL_BOLT_TASKS = "detail_bolt.tasks";
	public static final String DETAIL_BOLT_FLUSH_INTERVAL = "detail_bolt.flush_interval";

	// ReduceBolt 配置键
	public static final String REDUCE_BOLT_TASKS = "reduce_bolt.tasks";
	public static final String REDUCE_BOLT_FLUSH_INTERVAL = "reduce_bolt.flush_interval";
	public static final String REDUCE_BOLT_HBASE_RPT_GRD_WBUF_SIZE = "reduce_bolt.rpt_grd_wbuf_size";
	public static final String REDUCE_BOLT_HBASE_RPT_POS_WBUF_SIZE = "reduce_bolt.rpt_pos_wbuf_size";
	public static final String REDUCE_BOLT_HBASE_RPT_POS_DESC_WBUF_SIZE = "reduce_bolt.rpt_pos_desc_wbuf_size";
	public static final String REDUCE_BOLT_HBASE_RPT_SHOP_WBUF_SIZE = "reduce_bolt.rpt_shop_wbuf_size";

	// UV 去重位图长度(以2的指数表示)
	public static final int UV_BITMAP_K = 13;

	// 计算逻辑标识
	public static final int MOONBOX_CALC_DEFAULT = 0;
	public static final int MOONBOX_CALC_ETAO = 1;
	public static final int MOONBOX_CALC_LZ = 2;

	// public static String ZK_CONF_PREFIX = "/mb/pem/";
	// public static String ZK_CONF_PREFIX = "/jhc_t/pem/";
	public static String ZK_CONF_PREFIX = "/jhc_t_d/pem/";
	public static String ZK_SYNC_PREFIX = ZK_CONF_PREFIX + "sync/";

	// 实例zk配置节点的正则表达式
	public static Pattern INST_OPT_PATTERN = Pattern
			.compile("^(\\d+)\\.([\\w\\.]+)$");
	public static Pattern INST_OPT_FULL_PATTERN = Pattern.compile("^"
			+ ZK_CONF_PREFIX + "(\\d+)\\.([\\w\\.]+)$");

	// 实例配置选项
	public static String ZK_OPT_CLEAN_INACTIVE_USER = "opt.clean_inactive_user";
	public static String ZK_OPT_INACTIVE_USER_TIME = "opt.inactive_user_time";
	public static String ZK_OPT_CLEAN_USER_INTERVAL = "opt.clean_user_interval";
	public static String ZK_OPT_MAX_SCAN_ENTRIES = "opt.max_scan_entries";
	public static String ZK_OPT_MAX_ITEMS_PER_POS = "opt.max_items_per_pos";
	public static String ZK_OPT_DROP_INVALID_ID = "opt.drop_invalid_id";
	public static String ZK_OPT_DROP_INVALID_POS = "opt.drop_invalid_pos";
	public static String ZK_OPT_VALID_SHOPID_LIST = "opt.valid_shopid_list";
	public static String ZK_OPT_VALID_AUCTIONID_LIST = "opt.valid_auctionid_list";
	public static String ZK_OPT_VALID_POS_LIST = "opt.valid_pos_list";
	public static String ZK_CONF_URL_ID_LIST = "conf.url_id_list";
	public static String ZK_DISABLE = "disable";
	public static String ZK_TYPE = "type";

	// 全局配置选项
	public static String ZK_SYNC_ACTIVE_TRAFFIC_SPOUT = "sync/active_traffic_spout";
	public static String ZK_SYNC_ACTIVE_BUSINESS_SPOUT = "sync/active_business_spout";
	public static String ZK_SYNC_TRAFFIC_SPOUT_SLEEP_RECORD = "sync/traffic_spout_sleep_record";
	public static String ZK_SYNC_TRAFFIC_SPOUT_SLEEP_TIME = "sync/traffic_spout_sleep_time";
	public static String ZK_SYNC_BUSINESS_SPOUT_SLEEP_RECORD = "sync/business_spout_sleep_record";
	public static String ZK_SYNC_BUSINESS_SPOUT_SLEEP_TIME = "sync/business_spout_sleep_time";
}
