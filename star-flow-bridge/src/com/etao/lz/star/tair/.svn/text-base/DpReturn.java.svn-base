package com.etao.lz.star.tair;

import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.map.hash.TLongObjectHashMap;

import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.etao.lz.star.tair.TConfig;
import com.etao.lz.star.utils.AdaptiveCountingCheck;
import com.etao.lz.star.utils.GZipUtil;
import com.etao.lz.star.tair.GetTairClient;
import com.etao.lz.star.utils.TimeUtils;
import com.etao.lz.star.tair.ReturnValues;

import com.taobao.tair.DataEntry;
import com.taobao.tair.Result;
import com.taobao.tair.ResultCode;
import com.taobao.tair.etc.TairConstant;
import com.taobao.tair.impl.mc.MultiClusterExtendTairManager;

public class DpReturn implements java.io.Serializable {

	private static final long serialVersionUID = 4845307018902139592L;
	private long c = 0;
	// private ConcurrentHashMap<String, ReturnValues> unit_return = new
	// ConcurrentHashMap<String, ReturnValues>(); // 存储店铺
	private TLongObjectHashMap<ReturnValues> unit_return = new TLongObjectHashMap<ReturnValues>(); // 存储店铺回头客信息
	private static MultiClusterExtendTairManager rdbClient;
	public static String today_flag; // 防止跨天的时候，commit进程先于clear进程
	public static Log log = LogFactory.getLog(DpReturn.class);

	public int getDpReturn(String shopid, String cookie) {
		int dpreturn = 0;
		long id = Long.parseLong(shopid);
		ReturnValues returnval = (ReturnValues) unit_return.get(id);

		if (returnval == null) {

			returnval = new ReturnValues();
			getReturnInfo(returnval, id); // 获得店铺的回头客信息状态
			synchronized (unit_return) {
				try {
					
					unit_return.put(id, returnval);

				} catch (Exception e) {
					log.error(e.getStackTrace().toString());
				}
			}
			
		}

		putReturnValue(returnval, cookie); // 存入当天回头客信息

		dpreturn = getReturnValue(returnval, cookie); // 回头客

		// log.error("dp_return " + values[Config.INDEX_RETURN]); //tiaoshi

		c = c + 1;
		if (c % TConfig.commit_pv == 0) {

			String today = TimeUtils.getTodayDay();
			if (today.equals(today_flag)) {
				synchronized (unit_return) {
					try {
						commitReturnInfo(unit_return);
					} catch (Exception e) {
						log.error("commit returninfo  has failed !!!");
						e.printStackTrace();
					}

				}
			}

		}

		return dpreturn;
	}

	// 判断用户是否是回头客函数
	private int getReturnValue(ReturnValues returnval, String acookie) {

		if (returnval.historyvalue.checkoffer(acookie)) {

			// log.error("0  dp_return " +
			// returnval.historyvalue.cardinality()); //tiaoshi
			return 0;

		} else {
			// log.error("1  dp_return " +
			// returnval.historyvalue.cardinality());
			return 1;
		}

	}

	// 更新用户回头客信息
	private void putReturnValue(ReturnValues returnval, String acookie) {

		if (returnval.todayvalue.offer(acookie)) {

			returnval.needCommit = true; // 更新状态信息标志

		}

	}

	// 保存所有用户当天的回头客信息
	private synchronized void commitReturnInfo(
			TLongObjectHashMap<ReturnValues> unit_return2) {

		// 参数， 需要传 一个tair的链接对象，开发tair 写入逻辑

		// test 功能

		// Iterator<Entry<Long, ReturnValues>> itr = unit_return2.entrySet()
		// .iterator();
	//	TLongObjectIterator<ReturnValues> itr = unit_return2.iterator();
		for (TLongObjectIterator<ReturnValues> itr = unit_return2.iterator(); itr
				.hasNext();) {
			// while (itr.hasNext()) {
			// Map.Entry<Long, ReturnValues> entry = null;
			long unit_id = 0;
			ReturnValues reval = null;
			itr.advance();
			// entry = (Entry<Long, ReturnValues>) itr.next();
			unit_id = itr.key();
			reval = itr.value();

			if (reval.needCommit) {

				saveReturnStatus(reval, unit_id);
				reval.needCommit = false;
				// System.out.println("-----------commit   yi ci le ---------------- ");

				// }

			}
		}

	}

	// 保存单个店铺的回头客状态
	private void saveReturnStatus(ReturnValues reval, long unit_id) {

		// 采用rd, 普通结构，key -> value

		String day = today_flag;

		String rdb_key = String.format("%s_%s_return", unit_id, day);

		byte[] val = reval.todayvalue.getBytes();

		byte[] rdb_val = GZipUtil.GZip(val);

		ResultCode rcode = rdbClient.put(TConfig.namespace, rdb_key, rdb_val,
				TairConstant.NOT_CARE_VERSION, TConfig.return_expire_time);

		if (rcode.isSuccess()) {

			// System.out.println("sessions_bit_map " + rdb_val);

		} else {

			String errMes = String.format(
					"put  session_bit_map_rdb key : %s  not  success", rdb_key);
			log.error(errMes);
		}

	}

	// 获取用户所有的回头客信息 前6天 + 当天
	private void getReturnInfo(ReturnValues returnval, long unit_id) {

		getHistoryReturnInfo(returnval, unit_id);
		getTodayReturnInfo(returnval, unit_id);

	}

	// 获取用户前6天的回头客信息
	private void getHistoryReturnInfo(ReturnValues returnval, long unit_id) {

		// 采用rd, 普通结构，key -> value
		AdaptiveCountingCheck re = new AdaptiveCountingCheck(
				TConfig.UV_BITMAP_K);

		// 获取前6天的数据，并进行合并
		for (int i = 1; i < 7; i++) {

			String day = TimeUtils.getTodayDayAgo(i);

			String rdb_key = String.format("%s_%s_return", unit_id, day);

			Result<DataEntry> rcode = rdbClient.get(TConfig.namespace, rdb_key);

			if (rcode.isSuccess()) {

				if (rcode.getValue() != null) {
					// byte[] rdb_vals = rcode.getValue().getValue().toString()
					// .getBytes();
					byte[] rdb_vals = (byte[]) rcode.getValue().getValue();
					if (rdb_vals.length > 0) {
						byte[] rdb_val = GZipUtil.unGZip(rdb_vals);
						if (rdb_val != null) {
							AdaptiveCountingCheck tmp = new AdaptiveCountingCheck(
									rdb_val);

							try {

								re = new AdaptiveCountingCheck(re.merge(tmp)
										.getBytes());

							} catch (Exception e) {

								log.error("merge dpstat_return  status failed!!!");
								e.printStackTrace();

							}
						} else {

							String err = String
									.format("get  today  dp_return_bit_map_rdb key : %s  not  success, byte length is %s",
											rdb_key, rdb_vals.length);
							log.error(err);
						}
						// log.error("dp_return info  jia zai cheng gong !!! " +
						// i + "天前"); // tiaoshi
					}
				}

			} else {

				String errMes = String
						.format("get  today  dp_return_bit_map_rdb key : %s  not  success",
								rdb_key);
				log.error(errMes);
			}

		}

		returnval.historyvalue = re;

	}

	// 获取用户当天的回头客信息
	private void getTodayReturnInfo(ReturnValues returnval, long unit_id) {

		// 采用rd, 普通结构，key -> value

		String day = TimeUtils.getTodayDay();

		String rdb_key = String.format("%s_%s_return", unit_id, day);

		Result<DataEntry> rcode = rdbClient.get(TConfig.namespace, rdb_key);

		if (rcode.isSuccess()) {

			if (rcode.getValue() != null) {
				// byte[] rdb_vals = rcode.getValue().getValue().toString()
				// .getBytes();
				byte[] rdb_vals = (byte[]) rcode.getValue().getValue();
				if (rdb_vals.length > 0) {
					byte[] rdb_val = GZipUtil.unGZip(rdb_vals);
					if (rdb_val != null)
						returnval.todayvalue = new AdaptiveCountingCheck(
								rdb_val);

					// log.error("today  dp_return info  jia zai cheng gong !!! ");
					// // tiaoshi
				}
			}
		} else {

			String errMes = String.format(
					"get  today  dp_return_bit_map_rdb key : %s  not  success",
					rdb_key);
			log.error(errMes);
		}

	}

	/**
	 * 启动每天清零店铺状态定时器
	 */
	protected void startDpreturnResetTimer(
			final TLongObjectHashMap<ReturnValues> unit_return2) {
		log.info("======start_statusDpreturnResetTimer======");

		// timer job define
		TimerTask task = new TimerTask() {
			@Override
			public void run() {
				// refresh pv cache
				log.info(">>>start_statusdpreturnResetTimer begin");
				synchronized (unit_return2) {
					try {

						commitReturnInfo(unit_return2);
						// 清空访问缓存
						return_clear();

					} catch (Exception e) {
						log.error(e.getStackTrace().toString());
					}
					log.info("<<<start_statusDpReturnResetTimer finish");
				}
			}
		};

		long tomorrowBegin = TimeUtils.getTomorrowTimeBegin();
		long firstInterval = tomorrowBegin - System.currentTimeMillis();

		if (tomorrowBegin != 0) {
			Timer timer = new Timer();
			timer.schedule(task, firstInterval + 1000, 24 * (3600 + 1) * 1000);
			log.info("======startDpRturnResetTimer======");
		}
	}

	// 清空内存
	public void return_clear() {

		unit_return.clear();
		// rdbClient = GetTairClient.getRdbClient(); // 初始化，tair 客户端
		today_flag = TimeUtils.getTodayDay();
		c = 0;

	}

	public void init() {

		rdbClient = GetTairClient.getRdbClient(); // 初始化，tair 客户端
		today_flag = TimeUtils.getTodayDay();

		startDpreturnResetTimer(unit_return);

	}

}
