package com.etao.lz.star.bolt;

import java.util.HashSet;
import java.util.Map;

import com.etao.lz.star.StarConfig;
import com.etao.lz.star.StarLogProtos;
import com.google.protobuf.GeneratedMessage.Builder;

@SuppressWarnings("rawtypes") 
public class BusinessFillProcessor implements LogProcessor {
	private static final long serialVersionUID = -8519565002907282077L;
	private static HashSet<String> STATUS = new HashSet<String>();
	static {
		STATUS.add("TRADE_FINISHED");
		STATUS.add("TRADE_CLOSED");
		STATUS.add("TRADE_REFUSE");
		STATUS.add("TRADE_REFUSE_DEALING");
		STATUS.add("TRADE_CANCEL");
	}

	@Override
	public void open(int taskId) {
	}

	@Override
	public boolean accept(Builder builder) {
		return true;
	}

	@Override
	public void config(StarConfig config) {

	}

	@Override
	public void process(Builder builder) {
		StarLogProtos.BusinessStarLog.Builder log = (StarLogProtos.BusinessStarLog.Builder) builder;
		String dbAction = log.getDbAction();
		if (dbAction == null)
			return;
		
		String tradeStatus = log.getTradeStatus();
		//setting is_new_generated
		if (dbAction.startsWith("insert") && !STATUS.contains(tradeStatus)) {
			log.setIsNewGenerated(1);
		} else {
			log.setIsNewGenerated(0);
		}
		
		//setting is_pay
		log.setIsPay(0);
		
		String targetColumn = null;
		String logSrc = log.getLogSrc();
		
		if (dbAction.startsWith("insert")) {
			if (logSrc.equals("order_0000")) {
				if (tradeStatus.equals("TRADE_SUCCESS")
						|| tradeStatus.equals("WAIT_SELLER_SEND_GOODS")) {
					log.setIsPay(1);
				}
			}
		} else if(dbAction.startsWith("update:")){
			if (logSrc.equals("order_0000")) {
				targetColumn = "10";
			}else if (logSrc.equals("tc_biz_order") && log.getPayStatus() == 2) {
				targetColumn = "13";
			} else if (logSrc.equals("tc_pay_order") && log.getPayStatus() == 2) {
				targetColumn = "6";
			}
			if (targetColumn != null) {
				String[] columns = dbAction.substring(7).split(",");
				for (int i = 0; i < columns.length; ++i) {
					String column = columns[i].trim();
					if (column.equals(targetColumn)) {
						log.setIsPay(1);
						break;
					}
				}
			}
		}
	}

	@Override
	public void getStat(Map<String, String> map) {
	}
}
