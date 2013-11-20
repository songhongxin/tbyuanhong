package com.etao.lz.storm.utils;

import static org.junit.Assert.assertEquals;
import gnu.trove.map.hash.TLongObjectHashMap;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;

import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;


import org.yaml.snakeyaml.Yaml;

import com.etao.lz.star.ConvUtil;
import com.etao.lz.star.GenericLoader;
import com.etao.lz.star.GenericLoader.RawLogFmtDesc;
import com.etao.lz.star.StarLogProtos;
import com.etao.lz.star.StarLogProtos.BusinessStarLog;
import com.etao.lz.star.StarLogProtos.FlowStarLog;
import com.etao.lz.storm.detailcalc.AlizhgDetail;
import com.etao.lz.storm.detailcalc.JhcDetail;
import com.etao.lz.storm.detailcalc.ShopItem;
import com.etao.lz.storm.detailcalc.UnitItem;

public class MiscTestUtil {

	/**
	 * 加载 Ctrl-A 分隔的 QA 流量日志
	 * 
	 * @param logPath
	 * @return
	 * @throws Exception
	 */
	public static List<StarLogProtos.FlowStarLog> loadQAFlowLog(String logPath)
			throws Exception {
		RawLogFmtDesc fmtDesc = new RawLogFmtDesc("\001", "utf-8");
		return GenericLoader.genericLoadFlowDataToPB(logPath, fmtDesc);
	}

	/**
	 * 加载 Ctrl-A 分隔的 QA 业务日志
	 * 
	 * @param logPath
	 * @return
	 * @throws Exception
	 */
	public static List<StarLogProtos.BusinessStarLog> loadQABizLog(
			String logPath) throws Exception {
		RawLogFmtDesc fmtDesc = new RawLogFmtDesc("\001", "utf-8");
		return GenericLoader.genericLoadBizDataToPB(logPath, fmtDesc);
	}

	/**
	 * 根据给定结果 YAML 判断指标是否正确
	 * 
	 * @param title
	 *            注释标题
	 * @param indsYaml
	 *            保存正确结果的 YAML 文件
	 * @param inds
	 *            待比对指标
	 * @throws Exception
	 */

	// 江湖策检测

	public static void jhcdetailCalcDriver(JhcDetail dc, String flowLogPath,
			String bizLogPath, String reYaml, PrintStream tupleOut)
			throws Exception {
		// 给出流量日志时即加载数据并驱动 DetailCalc 实例
		long t = 0;
		if (flowLogPath != null) {
			List<FlowStarLog> flowLogs = MiscTestUtil
					.loadQAFlowLog(flowLogPath);
			for (FlowStarLog log : flowLogs) {
				long ts = log.getTs() / 1000;
				t = log.getTs();
				dc.updateTrafficIndicators(log, ts);

				// System.out.println(log.getMid());

			}
		}

		// 给出业务日志时即加载数据并驱动 DetailCalc 实例
		if (bizLogPath != null) {
			List<BusinessStarLog> bizLogs = MiscTestUtil
					.loadQABizLog(bizLogPath);
			for (BusinessStarLog log : bizLogs) {
				long ts = log.getOrderModifiedT() / 1000;
				dc.updateBusinessIndicators(log, ts);
				t = log.getOrderModifiedT();
			}
		}

		// 得到结果
		TLongObjectHashMap<UnitItem> curShopMap = dc.getCurItemMap();
		ArrayList<UnitItem> flushHourList = new ArrayList<UnitItem>();
		ArrayList<UnitItem> flushDayList = new ArrayList<UnitItem>();
		dc.updateItemMap(curShopMap, t, flushHourList, flushDayList);

		TLongObjectHashMap<UnitItem> item = change(flushDayList);
		
		

		// TLongObjectHashMap<UnitItem> item = dc.mergeItemMap(curShopMap, t);
		// System.out.println(curShopMap.get(111).iuv);
		// dumpOrAssert("jianghuce", reYaml, curShopMap);
		JhcIndexTest("jianghuce", reYaml, item);

		if (tupleOut != null) {
			Map<Integer, List<Object>> groupByGid = new TreeMap<Integer, List<Object>>();
			Set<Long> gids = new TreeSet<Long>();

			for (long k : curShopMap.keys()) {
				gids.add(k);
			}

			for (long gid : gids) {
				Object[] va = new Object[1];
				va[0] = ConvUtil.recurTroveToStd(curShopMap.get(gid), true);
				groupByGid.put((int) gid, Arrays.asList(va));
			}
			List<Map<Integer, List<Object>>> res = new ArrayList<Map<Integer, List<Object>>>();
			for (Entry<Integer, List<Object>> e : groupByGid.entrySet()) {
				Map<Integer, List<Object>> m = new TreeMap<Integer, List<Object>>();
				m.put(e.getKey(), e.getValue());
				res.add(m);
			}

			Yaml yaml = new Yaml();
			tupleOut.println("##### tuples #####");
			tupleOut.println(yaml.dump(res));
		}
	}

	private static TLongObjectHashMap<UnitItem> change(ArrayList<UnitItem> list) {

		TLongObjectHashMap<UnitItem> unititem = new TLongObjectHashMap<UnitItem>();

		for (int i = 0; i < list.size(); i++) {

			UnitItem item = list.get(i);
	//		long sellerid = item.seller_id;
			long auctionid = item.auctionId;
	//		UnitItem val_day = new UnitItem(sellerid, auctionid);

			unititem.put(auctionid, item);
		}
		// TODO Auto-generated method stub
		return unititem;
	}

	// 测试阿里指挥官单元测试
	public static void zhgdetailCalcDriver(AlizhgDetail dc, String flowLogPath,
			String bizLogPath, String reYaml, PrintStream tupleOut)
			throws Exception {

		// 给出业务日志时即加载数据并驱动 DetailCalc 实例
		if (bizLogPath != null) {
			List<BusinessStarLog> bizLogs = MiscTestUtil
					.loadQABizLog(bizLogPath);
			for (BusinessStarLog log : bizLogs) {
				long ts = log.getOrderModifiedT() / 1000;
				dc.updateBusinessIndicators(log, ts);
				System.out.println(log.getActualTotalFee());

			}
		}

		// 得到结果
		TLongObjectHashMap<ShopItem> curShopMap = dc.getCurShopMap();
		IndexTest("alizhg", reYaml, curShopMap);

		// dumpOrAssert("alizhg", reYaml, curShopMap);

		if (tupleOut != null) {
			Map<Integer, List<Object>> groupByGid = new TreeMap<Integer, List<Object>>();
			Set<Long> gids = new TreeSet<Long>();

			for (long k : curShopMap.keys()) {
				gids.add(k);
			}

			for (long gid : gids) {
				Object[] va = new Object[1];
				va[0] = ConvUtil.recurTroveToStd(curShopMap.get(gid), true);
				groupByGid.put((int) gid, Arrays.asList(va));
			}
			List<Map<Integer, List<Object>>> res = new ArrayList<Map<Integer, List<Object>>>();
			for (Entry<Integer, List<Object>> e : groupByGid.entrySet()) {
				Map<Integer, List<Object>> m = new TreeMap<Integer, List<Object>>();
				m.put(e.getKey(), e.getValue());
				res.add(m);
			}

			Yaml yaml = new Yaml();
			tupleOut.println("##### tuples #####");
			tupleOut.println(yaml.dump(res));
		}
	}

	// 开始检测江湖策官各个指标的计算情况
	private static void JhcIndexTest(String title, String reYaml,
			TLongObjectHashMap<UnitItem> curShopMap) {

		String yamlComment = "###### " + title + " ######";
		Yaml yaml = new Yaml();
		if (!reYaml.startsWith("/")) {
			reYaml = ClassLoader.getSystemResource(reYaml).getFile();
		}

		System.out.println(yamlComment);
		// Object curitem = ConvUtil.recurTroveToStd(curShopMap);
		try {
			@SuppressWarnings("unchecked")
			Map<Object, Object> exp = (Map<Object, Object>) yaml
					.load(new InputStreamReader(new FileInputStream(reYaml),
							"utf-8"));

			for (Entry<Object, Object> entry : exp.entrySet()) {
				// 序号id
				// long id = Long.parseLong(entry.getKey().toString());
				@SuppressWarnings("unchecked")
				Map<Object, Object> item = (Map<Object, Object>) entry
						.getValue();
				for (Entry<Object, Object> j : item.entrySet()) {

					long item_id = Long.parseLong(j.getKey().toString());
					@SuppressWarnings("unchecked")
					Map<Object, Object> y = (Map<Object, Object>) j.getValue();
					long t_pv = Long.parseLong(y.get("pv").toString());
					long t_an = Long.parseLong(y.get("an").toString());
					long t_atn = Long.parseLong(y.get("atn").toString());
					long t_amt = Long.parseLong(y.get("amt").toString());
					long t_tn = Long.parseLong(y.get("tn").toString());
					long t_az = Long.parseLong(y.get("az").toString());
					long t_ar = Long.parseLong(y.get("ar").toString());
					long t_au = Long.parseLong(y.get("au").toString());
					long t_auv = Long.parseLong(y.get("auv").toString());
					long t_iuv = Long.parseLong(y.get("iuv").toString());

					UnitItem shopitem = curShopMap.get(item_id);
					long amt = shopitem.alipayAmt;
					long pv = shopitem.pv;
					long an = shopitem.alipayNum;
					long atn = shopitem.alipayTradeNum;
					long tn = shopitem.tradeNum;
					long au = shopitem.uvValue;
					long az = shopitem.alipayRate;
					long ar = shopitem.dealRate;
					long auv = shopitem.ab.cardinality();
					long iuv = shopitem.ib.cardinality();

					assertEquals(t_amt, amt); // 检查支付宝成交金额
					assertEquals(t_pv, pv); // 检查pv
					assertEquals(t_an, an); // 检查购买件数
					assertEquals(t_atn, atn); // 检查购买笔数
					assertEquals(t_tn, tn); // 检查拍下笔数
					assertEquals(t_az, az); // 检查当日支付率
					assertEquals(t_ar, ar); // 检查成交转化率
					assertEquals(t_au, au); // 检查用户价值
					assertEquals(t_auv, auv); // 
					assertEquals(t_iuv, iuv); //

				}

			}

		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// TODO Auto-generated method stub

	}

	// 开始检测阿里指挥官各个指标的计算情况
	private static void IndexTest(String title, String reYaml,
			TLongObjectHashMap<ShopItem> curShopMap) {

		String yamlComment = "###### " + title + " ######";
		Yaml yaml = new Yaml();
		if (!reYaml.startsWith("/")) {
			reYaml = ClassLoader.getSystemResource(reYaml).getFile();
		}

		System.out.println(yamlComment);
		// Object curitem = ConvUtil.recurTroveToStd(curShopMap);
		try {
			@SuppressWarnings("unchecked")
			Map<Object, Object> exp = (Map<Object, Object>) yaml
					.load(new InputStreamReader(new FileInputStream(reYaml),
							"utf-8"));

			for (Entry<Object, Object> entry : exp.entrySet()) {
				// 序号id
				// long id = Long.parseLong(entry.getKey().toString());
				@SuppressWarnings("unchecked")
				Map<Object, Object> item = (Map<Object, Object>) entry
						.getValue();
				for (Entry<Object, Object> j : item.entrySet()) {

					long item_id = Long.parseLong(j.getKey().toString());
					@SuppressWarnings("unchecked")
					Map<Object, Object> y = (Map<Object, Object>) j.getValue();
					long t_amt = Long.parseLong(y.get("amt").toString());

					System.out.println(item_id);
					ShopItem shopitem = curShopMap.get(item_id);
					if (shopitem == null) {
						System.out.println("failture!!");
						return;
					}
					long amt = shopitem.alipayAmt;

					assertEquals(t_amt, amt); // 检查支付宝成交金额

				}

			}

		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// TODO Auto-generated method stub

	}

}
