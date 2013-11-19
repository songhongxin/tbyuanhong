package com.etao.lz.star.output.broadcast;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.jexl2.Expression;
import org.apache.commons.jexl2.JexlEngine;
import org.apache.commons.jexl2.MapContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.etao.lz.star.StarConfig;
import com.etao.lz.star.Tools;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.GeneratedMessage;

public class SendingState implements BroadcastState{
	private LinkedBlockingQueue<byte[]> queue;
	private MapContext context;
	private Expression expr;

	private static Log LOG = LogFactory.getLog(SendingState.class);
	private Set<FieldDescriptor> projection;
	@SuppressWarnings("rawtypes")
	private GeneratedMessage.Builder builder;
	private List<FieldDescriptor> fields;
	
	public SendingState(LinkedBlockingQueue<byte[]> queue, SubscriberInfo item, StarConfig config) {
		this.queue = queue;
		setFilter(item.getFilter());
		builder = config.newLogBuilder();
		fields = config.getDescriptor().getFields();
		setProjection(item.getProjection());
		
	}

	public void addData(byte[] data, GeneratedMessage log) {
		if (context != null && expr != null) {
			context.set("$log", log);
			try {
				Boolean ret = (Boolean) expr.evaluate(context);
				if (!ret.booleanValue())
					return;
			} catch (Exception e) {
				LOG.error(Tools.formatError(
						"SubscriberInfo Eval " + expr.getExpression(), e));
			}
		}
		if (projection == null || projection.size() == 0) {
			queue.add(data);
		} else {
			builder.clear();

			for (FieldDescriptor f : projection) {
				builder.setField(f, log.getField(f));
			}
			queue.add(builder.build().toByteArray());
		}
	}

	private void setFilter(String filter) {
		if(filter == null || filter.length() == 0)
		{
			context = null;
			expr = null;
			return;
		}

		JexlEngine engine = new JexlEngine();
		context = new MapContext();
		expr = engine.createExpression(filter);
	}
	
	private void setProjection(Set<String> proj_list)
	{
		if(proj_list != null)
		{
			projection = new HashSet<FieldDescriptor>();
			
			for(FieldDescriptor field: fields)
			{
				if(proj_list.contains(field.getName()))
				{
					projection.add(field);
				}
			}
		}
	}
}
