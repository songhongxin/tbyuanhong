package com.etao.lz.star.bolt;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;


public class NewRefParser {

	public static class NewRefPat {
		Pattern url; // 匹配url的正则表达式
		String src_id; // 来源页面的类型，如p4p，access等
		Pattern key; // 一个规则的多于一个的额外关键信息，往往是配合keypos使用来进行规则的准确定义
		String keypos; // 关健词所在的锚点信息
		String use; // 表示是否用"url"来匹配
		String full; // 是否是"1"，表示是否用整个url
		String host; // 表示是否用计算host
		String res; // 表示是否new
		String s1_id; // 来源页面的类型，如p4p，access等

		@Override
		public String toString() {
			return url + "☻" + src_id + "☻" + key + "☻" + keypos + "☻" + use
					+ "☻" + full + "☻" + host + "☻" + res + "☻" + s1_id;
		}
	}

	private static List<NewRefPat> pats = new ArrayList<NewRefPat>();

	private static final String KEY_NO = "0"; // 无关健词
	private static final String KEY_TAOBAO_NORMAL = "1"; // 淘宝主搜关健词
	private static final String KEY_TAOBAO_P4P = "2"; // “p4p”直通车推广关健词
	//private static final String KEY_TAOBAO_SHOP = "3"; // 淘宝店内搜索关健词
	private static final String KEY_ENGINE = "4"; // 站外搜索引擎关健词，如百度
	private static final String KEY_TAOBAO_TK = "5"; // 淘客搜索关健词
	private static final String KEY_TAOBAO_TMALL = "6"; // 商城搜索关健词

	private static final String REF_SRCID_P4P = "11"; // 新来源的直通车
	private static final String REF_SRCID_TBPS = "20"; // 新来源的淘宝宝贝搜索
	private static final String REF_SRCID_TKS = "33"; // 新来源的淘客搜索标识
	private static final String REF_SRCID_TMPS = "36"; // 新来源的天猫宝贝搜搜

	private static final String REF_SRCID_TAOBAO = "19"; // 1级，新来源的淘宝站内
	private static final String REF_SRCID_SEARCH = "61"; // 1级，新来源的站外引擎
	private static final String REF_SRCID_AD = "8"; // 1级，新来源的付费来源

	private static final String REF_SRCID_TAOKE = "10"; // 新来源的淘客标识
	private static final String REF_SRCID_DIANPU = "30"; // 新来源中的其他店铺标识
	private static final String REF_SRCID_TBUNKNOWN = "59"; // 新来源中的站内其他标识
	private static final String REF_SRCID_UNKNOWN = "74"; // 新来源中的站外其他标识

	private static final String TBKQT = "淘宝客其他";

	private static final Pattern REF_PAT = Pattern.compile(
			"^http(s)?:\\/\\/(.+?)(\\/|$)", Pattern.CASE_INSENSITIVE);
	private static final Pattern REF_PAT2 = Pattern
			.compile(
					"^http:\\/\\/s\\.click\\.taobao\\.com\\/(t_js|js)\\?(.+&)?tu=(.*?)($|&)",
					Pattern.CASE_INSENSITIVE);

	static {
		try {
			SAXBuilder builder = new SAXBuilder();
			Document doc = builder.build(NewRefParser.class.getClassLoader()
					.getResourceAsStream("flow_bridge/new_ref.xml"));
			Element root = doc.getRootElement(); // xmlconf

			for (Object o1 : root.getChildren()) {
				Element s1 = (Element) o1; // 1级类别，如淘宝站内
				for (Object o2 : s1.getChildren()) {
					Element s2 = (Element) o2; // 2级类别，如商场专题
					for (Object o3 : s2.getChildren("patten")) {
						Element s3 = (Element) o3; // pattern，3级类别，即不同规则的情况

						NewRefPat pat = new NewRefPat();
						pat.s1_id = getValue(s1, "src_id");
						pat.url = Pattern.compile(getValue(s3, "url"),
								Pattern.CASE_INSENSITIVE);
						pat.src_id = getValue(s3, "src_id");
						pat.key = Pattern.compile(getValue(s3, "key"),
								Pattern.CASE_INSENSITIVE);
						;
						pat.keypos = getValue(s3, "keypos");
						pat.use = getValue(s3, "use");
						pat.full = getValue(s3, "full");
						pat.host = getValue(s3, "host");
						pat.keypos = getValue(s3, "keypos");
						pat.res = getValue(s3, "res");

						pats.add(pat);
					}
				}
			}
		} catch (JDOMException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static HashMap<String, String> getInfo(String url, String ref,
			String shop_id) {
		HashMap<String, String> info = new HashMap<String, String>();
		String ref_pre = get_url_pre(ref);
		info.put("src_id", REF_SRCID_UNKNOWN);
		info.put("host", "");
		info.put("true_ref", "");
		info.put("key", "");
		info.put("key_type", KEY_NO);

		for (NewRefPat pat : pats) {
			// 分别获得new_ref.xml中各规则的不同信息
			Pattern p = pat.url;
			String src_id = pat.src_id.replaceAll("\\s+", " ").replaceAll(
					"(\\u0005)+", "");
			String s1_id = pat.s1_id.replaceAll("\\s+", " ").replaceAll(
					"(\\u0005)+", "");
			String host = pat.host;

			String avail_url = "";
			if (pat.use.equals("url")) {
				avail_url = url;
			} else {
				avail_url = pat.full.equals("1") ? ref : ref_pre;
			}
			// 不是直接访问
			if (p.pattern().equals("") == false) {
				Matcher m = p.matcher(avail_url);
				if (m.find()) {
					info.put("src_id", src_id);

					// 提取相关的关健词信息
					if (s1_id.equals(REF_SRCID_AD)) {
						if (src_id.equals(REF_SRCID_P4P)) {

							String url_decoded = urldecode(url);
							Matcher key_k = pat.key.matcher(url_decoded);
							if (key_k.find()) {

								info.put("key_type", KEY_TAOBAO_P4P);
								String raw_key = get_match_group(key_k,pat.keypos);
								String key = convert_str_to_utf8(raw_key);
								
								String type_num = get_match_group(key_k,"1");
								if (key.equals("")) {
									if (type_num.equals("6")) {
										info.put("key", "类目搜索");
									} else if (type_num.equals("7")) {
										info.put("key", "定向推广");
									}
								} else {
									info.put("key", key);
								}
							}
						}
					} else if (s1_id.equals(REF_SRCID_TAOBAO)) {
						if (src_id.equals(REF_SRCID_TBPS)
								|| src_id.equals(REF_SRCID_TMPS)
								|| src_id.equals(REF_SRCID_TKS)) {

							Matcher key_k = pat.key.matcher(ref);
							if (key_k.find()) {

								String raw_key =  get_match_group(key_k,pat.keypos);
								if (pat.res.equals("new")) {
									// 暂时啥也不错

								} else {
									// 暂时啥也不错
								}
//								System.out.println(raw_key);
								String key_utf8 = convert_str_to_utf8(raw_key);

								if (!key_utf8.equals("")) {

									info.put("key", key_utf8);
									if (src_id.equals(REF_SRCID_TBPS)) {

										info.put("key_type", KEY_TAOBAO_NORMAL);
									} else if (src_id.equals(REF_SRCID_TMPS)) {
										info.put("key_type", KEY_TAOBAO_TMALL);
									} else {
										info.put("key_type", KEY_TAOBAO_TK);
									}
								}
							}
						}

					} else if (s1_id.equals(REF_SRCID_SEARCH)) {
						Matcher key_k = pat.key.matcher(ref);
						if (key_k.find()) {
							String raw_key =  get_match_group(key_k,pat.keypos);
							String key_utf8 = convert_str_to_utf8(raw_key);
							info.put("key_type", KEY_ENGINE);
							info.put("key", key_utf8);

						}

					}
					

					String key_c = key_proc(info.get("key"));

					if (key_c.equals("")) {

						info.put("key_type", KEY_NO);
					}
					
					
					// 提取相关的ref_url信息
					if (host.equals("1")) {
						// 淘客ref，要从url里面获得
						if (src_id.equals(REF_SRCID_TAOKE)) {
							Matcher m1 = REF_PAT2.matcher(ref);
							if (m1.find()) {
								String raw_tu = get_match_group(m1,"3");;
								String url_decoded = urldecode(raw_tu);
								Matcher key_m = pat.key.matcher(url_decoded);
								if (key_m.find()) {
									String raw_ref = urldecode(get_match_group(key_m,pat.keypos));
									if (REF_PAT.matcher(raw_ref).find()) {
										info.put("true_ref", raw_ref);
										info.put("host", raw_ref);
										return info;
									}
								}
								info.put("host", TBKQT);
								return info;
							}
							Matcher m2 = REF_PAT.matcher(ref);
							if (m2.find()) {
								info.put("host", ref);
								return info;
							} else
								info.put("host", TBKQT);
							return info;
						}
						if (src_id.equals(REF_SRCID_DIANPU)) {
							String url_decoded = urldecode(avail_url);
							Matcher key_m = pat.key.matcher(url_decoded);
							if (key_m.find()) {
								String raw_shop_id = get_match_group(key_m,pat.keypos);
								if (shop_id.equals(raw_shop_id)) {
									info.put("src_id", REF_SRCID_TBUNKNOWN);
								}
								info.put("host", urldecode(avail_url)); // 下钻到url
								// info.put("host",
								// get_ref_host(urldecode(avail_url)));
							}
						}
						info.put("host", ref); // 下钻到url
						// info.put("host", urldecode(avail_url)); //下钻到url
						// info.put("host", get_ref_host(urldecode(avail_url)));
						return info;
					}

					return info;
				}
			} else { // 直接访问
				// 来源为空
				if (ref.equals("")) {
					info.put("src_id", src_id);
					return info;
				}
			}
		}
		return info;
	}
/*
	// 获得ref的域名
	private static String get_ref_host(String ref) {
		Matcher m = REF_PAT.matcher(ref);
		return m.find() ? m.group(2) : TBKQT;
	}

	// 获得来源url
	private static String get_taoke_host(String ref) {
		Matcher m = REF_PAT.matcher(ref);
		if (m.find()) {
			return m.group(2);
		}
		return ref.equals("") ? "" : "本地文件";
	}
*/
	// 简单的urlencode
	private static String urldecode(String url) {
		String result = "";
		try {
			result = URLDecoder.decode(url, "gbk");
		} catch (UnsupportedEncodingException e) {
			System.err.println("NewRefParser中未能正常decode的url: " + url);
		} catch (IllegalArgumentException e) {
		//	System.err.println("NewRefParser中未能正常decode的url: " + url);
		}
		return result.equals("") ? url : result;
	}

	// 获得url第一个"?"之前的部分
	private static String get_url_pre(String url) {
		int index = url.indexOf("?");
		return (index < 0) ? url : url.substring(0, index);
	}

	private static String getValue(Element e, String name) {
		String value = e.getAttributeValue(name);
		return (value == null) ? "" : value;
	}

	private static String convert_str_to_utf8(String key) {

	//	return  urldecode(EscapeTools.unescape(key));
		return urldecode(key);
	}

	private static String key_proc(String key) {
        String  re = "";
        
        re = key.replaceAll("\\s+", " ").replaceAll(
				"(\\u0005)+", "");
        
        re = re.replaceAll("/()+/", "$1");
		return re;
	}

	private static String get_match_group(Matcher m, String pos) {
 
        if(m.group(Integer.parseInt(pos)) != null)
        {
        	return  m.group(Integer.parseInt(pos));
        	
        }else
        	return "";
       

	}
	
	/*
	
	public static void main(String[] args) throws Exception {
		String ref = "http://list.tmall.com/search_product.htm?q=%BF%D5%B5%F7&user_action=initiative&at_topsearch=1&sort=st&type=p&cat=&style=";
	    String url  = "http://item.taobao.com/item.htm?id=17721922561&ali_trackid=:mm_30865935_0_0,20876:1373507458_6k3_802574853";

		HashMap <String, String>  c = new HashMap<String,String>();
		 c  = getInfo(url, ref, "3290980");
		System.out.println(c.size());
		System.out.println("key: " + c.get("key"));
		System.out.println("key_type: " + c.get("key_type"));
		System.out.println("src_id : " + c.get("src_id"));
		System.out.println("host : " + c.get("host"));
		
		
		String   hi   = "/w  ww      www /  ";
		System.out.println(key_proc(hi));
		System.out.println(hi.indexOf("%"));
		if(key_proc(hi).equals(" ")){
			
			System.out.println("chengle");
		}

	}
*/
}
