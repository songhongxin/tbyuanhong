package com.etao.lz.star.bolt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.etao.lz.star.StarConfig;
import com.etao.lz.star.StarLogProtos;
import com.etao.lz.star.Tools;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.GeneratedMessage.Builder;

@SuppressWarnings("rawtypes") 
public class RegExprProcessor implements LogProcessor {
	private static class PatternList
	{
		private List<Item> items;
		public List<Item> getItems() {
			return items;
		}
		private int unmatch_count;
		public PatternList() {
			items = new ArrayList<Item>();
		}
		public void add(Item item) {
			items.add(item);
		}
	}
	private static class Item {
		private Pattern pat;
		private int group;
		private int count;
		public Item(int group, Pattern pat) {
			this.group = group;
			this.pat = pat;
		}

		public Pattern getPattern() {
			return pat;
		}

		public int getGroup() {
			return group;
		}
	}

	private static final long serialVersionUID = -7864815391641225536L;

	private transient Map<String, PatternList> patternMap;
	private transient FieldDescriptor[] output_fields;
	private StarConfig config;
	private transient FieldDescriptor input_field;

	@Override
	public void open(int taskId) {
		String input = config.get("input_field");
		Map<String, FieldDescriptor> map = new HashMap<String, FieldDescriptor>();
		for (FieldDescriptor field : Tools.getDescriptor(config.getLogType())
				.getFields()) {
			map.put(field.getName(), field);
		}
		input_field = map.get(input);
		if (input_field == null) {
			throw new IllegalStateException("Unknown input field:" + input);
		}

		String[] fields = config.get("output_field").split(",");
		output_fields = new FieldDescriptor[fields.length];
		for(int i=0;i<fields.length;++i)
		{
			output_fields[i] =  map.get(fields[i].trim());
		}
		patternMap = new HashMap<String, PatternList>();
		for (String field : fields) {
			PatternList items = new PatternList();
			String[] patterns = config.get("regexpr_" + field).split("\n");
			for (String pattern : patterns) {
				pattern = pattern.trim();
				if (pattern.length() == 0)
					continue;

				int i = pattern.indexOf(',');
				if (i < 0) {
					throw new IllegalStateException("Missing group:" + pattern);
				}
				String group = pattern.substring(0, i).trim();
				String pat = pattern.substring(i + 1).trim();
				
				Pattern compile = Pattern.compile(pat, Pattern.CASE_INSENSITIVE);
				try {
					items.add(new Item(Integer.parseInt(group), compile));
				} catch (NumberFormatException e) {
					throw new IllegalStateException("Wrong group:" + group);
				}
			}
			patternMap.put(field, items);
		}
	}

	@Override
	public boolean accept(Builder builder) {
		StarLogProtos.FlowStarLog.Builder log = (StarLogProtos.FlowStarLog.Builder) builder;

		return log.getUnitId() > 0;
	}

	@Override
	public void config(StarConfig config) {
		this.config = config;
	}

	@Override
	public void process(Builder builder) {
		if(!builder.hasField(input_field)) return;
		Object field = builder.getField(input_field);
		if (field == null) return;
		for (FieldDescriptor out : output_fields) {
			builder.setField(out, "");
			PatternList patternList = patternMap.get(out.getName());
			boolean match = false;
			for (Item item : patternList.getItems()) {
				Matcher matcher = item.getPattern().matcher(field.toString());
				if (matcher.find()) {
					String s = matcher.group(item.getGroup());
					builder.setField(out, s);
					++item.count;
					match = true;
					break;
				}
			}
			if(!match)
			{
				++patternList.unmatch_count;
			}
		}
	}

	@Override
	public void getStat(Map<String, String> map) {
		
		for (FieldDescriptor out : output_fields) {
			PatternList patternList = patternMap.get(out.getName());
			StringBuilder sb = new StringBuilder();
			sb.append("unmatch" + "=" + String.valueOf(patternList.unmatch_count));
			sb.append("\n");
			List<Item> items = patternList.getItems();
			for (int i=0;i<items.size();++i) {
				Item item = items.get(i);
				sb.append("Pattern_");
				sb.append(i + 1);
				sb.append("_Count =");
				sb.append(item.count);
				sb.append("\n");
			}
			map.put(out.getName(), sb.toString());
		}
	}

}
