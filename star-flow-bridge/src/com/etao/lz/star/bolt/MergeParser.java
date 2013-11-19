package com.etao.lz.star.bolt;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jdom.Document;
import org.jdom.Element;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;

import sun.misc.BASE64Decoder;


public class MergeParser {
    
    public static class MergePat {
        Pattern url; // 匹配url的正则表达式
        String keypos; // 该url关键信息在url串中的位置
        String keyname; // 该url关键信息,如果该值指定了，则就忽略keypos了
        String titlepos; // 该url名字在url串中的位置
        String titlename; // 该url名字，如果该值指定了，则就忽略titlepos了
        String sppos; // 店内搜索起始价格在url串中的位置
        String eppos; // 店内搜索结束价格在url串中的位置
        Pattern except; // 不该含有的字符串正则表达式
        String type;    // 通过type去映射表中查找页面类型;

        @Override
        public String toString() {
            return url + "☻" + keypos + "☻" + keyname + "☻" + titlepos + "☻"
                    + titlename + "☻" + sppos + "☻" + eppos + "☻" + except;
        }
    }

    private static List<MergePat> pats = new ArrayList<MergePat>();
    
    private static final String UNKNOWN_PAGE_ID = "0"; // 被访页面为位置类型
	private static final String SEARCH_PAGE_ID = "1"; // 店内搜索页
	private static final String CAT_PAGE_ID = "2"; // 分类页
	private static final String ALL_PAGE_ID = "3"; // 所有宝贝页
	private static final String PRODUCT_PAGE_ID = "4"; // 宝贝页
	private static final String LABEL_PAGE_ID = "5"; // 标签页
//	private static final String COMM_PAGE_ID = "6"; // 交流区
	private static final String FIRST_PAGE_ID = "7"; //店铺首页
    
	private static final String KEY_NO = "0"; // 无关健词
	private static final String KEY_TAOBAO_SHOP = "3"; // 淘宝店内搜索关健词
	private static Log LOG = LogFactory.getLog(MergeParser.class);
	private static BASE64Decoder  d  =  new   BASE64Decoder();

   
    static {
        try {
            SAXBuilder builder = new SAXBuilder();
            Document doc = builder.build(MergeParser.class.getClassLoader()
                    .getResourceAsStream("flow_bridge/merge.xml"));
            Element root = doc.getRootElement(); // xmlconf
            Element taobao = root.getChild("taobao"); // taobao

            for (Object o2 : taobao.getChildren()) {
                Element e2 = (Element) o2; // search_page, category_page...
                String type = e2.getAttributeValue("type");

                for (Object o3 : e2.getChildren()) {
                    Element e3 = (Element) o3; // pattern

                    MergePat pat = new MergePat();
                    pat.url = Pattern.compile(getValue(e3, "url"), Pattern.CASE_INSENSITIVE);
                    pat.keypos = getValue(e3, "keypos");
                    pat.keyname = getValue(e3, "keyname");
                    pat.titlepos = getValue(e3, "titlepos");
                    pat.titlename = getValue(e3, "titlename");
                    pat.sppos = getValue(e3, "sppos");
                    pat.eppos = getValue(e3, "eppos");
                    pat.except = Pattern.compile(getValue(e3, "except"), Pattern.CASE_INSENSITIVE);
                    pat.type = type;

                    pats.add(pat);
                }
            }
            LOG.info("merge xml  has been loaded !");
        } catch (JDOMException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    // 提取被访页面的相关类型，以及宝贝id,分类页id
    public static HashMap<String, String> getInfo(String url, String title) {
       
    	
        String url_after = "";
        url_after  = url.replaceAll("=null", "=").replaceAll(" ", "");
               
        HashMap<String, String> info = new HashMap<String, String>();
        info.put("page_type", UNKNOWN_PAGE_ID);
        info.put("key_info", "");   //url md5
        info.put("title_info", get_page_name(title));
        info.put("key", "");
        info.put("key_type", KEY_NO);
        
        for (MergePat pat : pats) {
			// 分别获得merge.xml中各规则的不同信息
			Pattern p = pat.url;
			String  type = pat.type;
			String  keypos = pat.keypos;
			String  keyname = pat.keyname;
			String  titlepos  = pat.titlepos;
			String  titlename  = pat.titlename;
			String  sspos  =  pat.sppos;
			String  eepos  =  pat.eppos;
			Pattern  except =  pat.except;
			
			Matcher m = p.matcher(url_after);
			if (m.find())
			{
				//如果有模板中指定的非法字符串，则忽略该规则，匹配别的规则
				if(!except.pattern().equals(""))
				{ 
					Matcher m_e = except.matcher(url_after);
					if(m_e.find())
						continue;	
				}
				
				if(keyname.equals("") == false){
					
					info.put("key_info", keyname);
					
				}
				
				if(titlename.equals("") == false){
					
					info.put("title_info", titlename);
					
				}
				
				info.put("page_type", type);//所属页面类型
				
				
				if(type.equals(FIRST_PAGE_ID))
				{
					
				}else if(type.equals(ALL_PAGE_ID))
				{
					
				}else if(type.equals(CAT_PAGE_ID))
				{
					if(!keypos.equals(""))
					{
						 String tmp_k = m.group(Integer.parseInt(keypos));
						 if(tmp_k != null && !tmp_k.equals(""))
						 info.put("key_info", tmp_k);
				
					}
					
					if(!titlepos.equals(""))
					{
						String cat_name = "";
						if(m.group(Integer.parseInt(titlepos)) != null)
							try {
								cat_name =get_cat_name(m.group(Integer.parseInt(titlepos)));
							} catch (NumberFormatException e) {
								
								e.printStackTrace();
							} catch (IOException e) {
							
								e.printStackTrace();
							}
						cat_name  = get_preg_str(cat_name);
						if(!cat_name.equals(""))
						{
							info.put("title_info", cat_name);
							
						}
					}
					
					
				}else if(type.equals(PRODUCT_PAGE_ID))
				{
					if(!keypos.equals(""))
					{
						 String tmp_k = m.group(Integer.parseInt(keypos));
						 if(!tmp_k.equals(""))
						 info.put("key_info", tmp_k);
				
					}
					
					String  bname  = get_baobei_name(title);
					if(!bname.equals("")){
						info.put("title_info", bname);
					}
					
				}else if(type.equals(SEARCH_PAGE_ID))
				{
					String search_key = "";
					String search_price = "";
					String start_price = "";
					String end_price = "";
					int sp_flag = 0;
					
					if(!keypos.equals(""))
					{
						 String tmp_k = "";
						 if(m.group(Integer.parseInt(keypos)) != null)
						 {
							 tmp_k = m.group(Integer.parseInt(keypos)).trim();
						 }
						 search_key = urldecode(tmp_k);
						 search_key = get_preg_str(search_key);

					}
					if(!sspos.equals(""))
					{
						 String tmp_sp = "";
						 if(m.group(Integer.parseInt(sspos)) != null){
							 
							 tmp_sp  = m.group(Integer.parseInt(sspos)).trim();
						 }
	
						 start_price = tmp_sp;
						 search_price = start_price + "~";
                         sp_flag = 1;
					}
					
					if(!eepos.equals(""))
					{
						 String tmp_ed = "";
						 if(m.group(Integer.parseInt(eepos)) != null)
						 {
							 tmp_ed = m.group(Integer.parseInt(eepos)).trim();
						 }
						 end_price = tmp_ed;
						 if(sp_flag == 1)
						 {
							 search_price = search_price + end_price; 
						 }else
						 {
							 search_price = "~" + end_price;
						 }
					}
					
					search_price  = get_preg_str(search_price);
					
					if(!search_key.equals("") || !search_price.equals(""))
					{
						if(search_key.indexOf("%") < 0  && search_price.indexOf("%") < 0)
						{
							 String   keyp = key_proc(search_key + "||" + search_price);
							 info.put("key", keyp);
							 info.put("key_type", KEY_TAOBAO_SHOP);
						}
					}
					
				}else if(type.equals(LABEL_PAGE_ID))
				{
					// nothing to do 
					
				}else
				{
					// nothing to do 
				}
				
				
				if((type.equals(SEARCH_PAGE_ID) ) && (info.get("key").equals("")))
				{
			        info.put("page_type", UNKNOWN_PAGE_ID);
			        info.put("key_info", "");   //url md5
			        info.put("title_info", get_page_name(title));
			        info.put("key", "");
			        info.put("key_type", KEY_NO);
			        continue;
				}
				
				return   info;
				
			}

        }
   
        return info;
    }

    //获取xml节点属性值
    private static String getValue(Element e, String name) {
        String value = e.getAttributeValue(name);
        return (value == null) ? "" : value;
    }
    
    
    
    //获取淘宝页面的标题
    private static String get_page_name(String title) {
    	int index = title.indexOf("-");
    	return (index < 0) ? title : title.substring(0, index);
    	
    }
    
    //做些字符串预处理
    private static String get_preg_str(String preg) {
    	String re =  preg.replaceAll("\\t+", " ").replaceAll("\\s+", " ");
    	
    	String ree   =  re.replaceAll("\\|+", "");
    	return ree;
    	
    }
    
    //获取分类页标题
    private static String get_cat_name(String cat) throws IOException {
    	
    	Pattern  p = Pattern.compile("^[a-zA-Z0-9\\/+=]+$");
    	
    	String   title  = urldecode(cat);
 
    	Matcher  m   =  p.matcher(title);
    	if(m.find()){
    
    		return  new String(d.decodeBuffer(title));

    	}else
    	{
    		return  title;
    	}
    	
    }
    
    //获取宝贝页标题
    private static String get_baobei_name(String title) {
    	
    	Pattern  p = Pattern.compile("^(.+)-淘宝网$");
    	
    	Matcher  m   =  p.matcher(title);
    	if(m.find()){
    		
    		if(m.group(1) != null)
    		{
        		return m.group(1);
    		}
            return  "";
    		
    	}else
    	{
    		return  title;
    	}
    	
    }
    
    
 // 简单的urldecode
	public static String urldecode(String url) {
		String result = "";
		try {
			result = URLDecoder.decode(url, "gbk");
		} catch (UnsupportedEncodingException e) {
			System.err.println("MergParser中未能正常decode的url: " + url);
		} catch (IllegalArgumentException e) {
		//	System.err.println("MergParser中未能正常decode的url: " + url);
		}
		return result.equals("") ? url : result;
	}
    
    //简单的关键词字符串预处理
	private static String key_proc(String key) {
        String  re = "";
        
        re = key.replaceAll("\\s+", " ").replaceAll(
				"(\\u0005)+", "");
        
        re = re.replaceAll("/()+/", "$1");
		return re;
	}
    
	/*
    public static void main(String[] args) throws Exception {
    	
    	String url  = "http://chanzy.tmall.com/shop/view_shop.htm";
    	String title = urldecode(EscapeTools.unescape("%u9996%u9875-%u5A75%u4E4B%u4E91%u5185%u8863%u65D7%u8230%u5E97--%20%u5929%u732BTmall.com"));
    	
		HashMap <String, String>  c = new HashMap<String,String>();
		
		System.out.println(urldecode("中性-粉红小铺 化妆品店 100%正品保证-淘宝网"));
		System.out.println(title);
		 c  = getInfo(url, title);
		System.out.println(c.size());
		System.out.println("key: " + c.get("key"));
		System.out.println("page_type: " + c.get("page_type"));
		System.out.println("key_type: " + c.get("key_type"));
		System.out.println("key_info : " + c.get("key_info"));
		System.out.println("title_info : " + c.get("title_info"));
		
        
    }
*/
}
