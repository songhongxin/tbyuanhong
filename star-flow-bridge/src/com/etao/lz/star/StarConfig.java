package com.etao.lz.star;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.etao.lz.star.StarLogProtos.BusinessStarLog;
import com.etao.lz.star.StarLogProtos.FlowStarLog;
import com.etao.lz.star.spout.LogMapping;
import com.etao.lz.star.spout.LogMappingItem;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.GeneratedMessage.Builder;

@SuppressWarnings("rawtypes")
public class StarConfig implements Serializable {
	private static final long serialVersionUID = -2804255031916180552L;

	public static final String StormZookeeperCluster = "StormZookeeperCluster";

	private Map<String, String> attributes;
	private LogMapping logMapping;

	public LogMapping getLogMappings() {
		return logMapping;
	}

	private String topo_name;

	public StarConfig(String topo_name) {
		this.topo_name = topo_name;
		attributes = new HashMap<String, String>();
		logMapping = new LogMapping();
	}

	public StarConfig(String topo_name, StarConfig parent) {
		this.topo_name = topo_name;
		attributes = new HashMap<String, String>(parent.attributes);
		logMapping = new LogMapping();
	}

	public String getLogType() {
		return attributes.get("LogType");
	}

	public String getTopoName() {
		return topo_name;
	}

	public String get(String attr) {
		if (!attributes.containsKey(attr)) {
			throw new IllegalStateException(attr + " not found!");
		}
		return attributes.get(attr).trim();
	}

	public String getLogClass()
	{
		String logClass = get("log_class");
		if (logClass == null || logClass.trim().equals("")) {
			throw new IllegalStateException("Wrong log class config");
		}
		return logClass.trim().toLowerCase();
	}
	
	public int getInt(String attr) {
		return Integer.parseInt(get(attr));
	}

	public int getInt(String attr, int defValue) {
		return Integer.parseInt(get(attr, String.valueOf(defValue)));
	}

	public String get(String attr, String defValue) {
		if (!attributes.containsKey(attr)) {
			return defValue;
		}
		return attributes.get(attr).trim();
	}

	public void addAttribute(String name, String value) {
		attributes.put(name, value);
	}

	public static Builder createBuilder(String logClass) {
		if (logClass.equals("flow")) {
			return StarLogProtos.FlowStarLog.newBuilder();
		} else {
			return StarLogProtos.BusinessStarLog.newBuilder();
		}
	}

	public long getLogTime(GeneratedMessage msg) {
		if (getLogType().equals("flow")) {
			FlowStarLog log = (FlowStarLog) msg;
			return log.getTs();
		} else {
			BusinessStarLog log = (BusinessStarLog) msg;
			return log.getOrderModifiedT();
		}
	}
	public long getLogTime(GeneratedMessage.Builder msg) {
		if (getLogType().equals("flow")) {
			FlowStarLog.Builder log = (FlowStarLog.Builder) msg;
			return log.getTs();
		} else {
			BusinessStarLog.Builder log = (BusinessStarLog.Builder) msg;
			return log.getOrderModifiedT();
		}
	}
	public String getDestType(GeneratedMessage msg) {
		if (getLogType().equals("flow")) {
			FlowStarLog log = (FlowStarLog) msg;
			return log.getDestType();
		} else {
			BusinessStarLog log = (BusinessStarLog) msg;
			return log.getDestType();
		}
	}

	private void doParseLogMapping(List<LogMappingItem> logMapping, Node mappingNode) {
		NamedNodeMap attr = mappingNode.getAttributes();
		if (attr == null)
			return;
		Node node = attr.getNamedItem("name");
		if (node == null) {
			throw new IllegalStateException("LogMapping missing attribute name");
		}
		String name = node.getTextContent();

		Node valueNode = attr.getNamedItem("value");
		if (valueNode == null) {
			throw new IllegalStateException("Missing value atrribute");
		}
		logMapping.add(new LogMappingItem(name, valueNode.getTextContent()));
	}

	private void parseAttributes(XMLParser xml, NodeList attrNodes)
			throws Exception {
		for (int i = 0; i < attrNodes.getLength(); ++i) {
			Node item = attrNodes.item(i);
			String name = xml.getString(item, "@name");
			String value = null;
			if (item.getAttributes().getNamedItem("value") != null) {
				value = xml.getString(item, "@value");
			} else {
				value = item.getTextContent();
			}
			attributes.put(name, value);
		}
	}

	public void parseCommon(XMLParser xml) throws Exception {
		NodeList attrNodes = xml.getNodeList(null,
				"CommonConfig/Configuration/Attribute");
		parseAttributes(xml, attrNodes);
	}

	public void parse(XMLParser xml, Node node) throws Exception {
		NodeList attrNodes = xml.getNodeList(node, "Configuration/Attribute");
		parseAttributes(xml, attrNodes);

		NodeList childNodes = xml.getNodeList(node, "LogMapping");
		for (int j = 0; j < childNodes.getLength(); ++j) {
			Node mappingNode = childNodes.item(j);
			String name = xml.getString(mappingNode, "@name");
			List<LogMappingItem> mapping = new ArrayList<LogMappingItem>();
			logMapping.put(name, mapping);
			NodeList mappingList = mappingNode.getChildNodes();
			for (int i = 0; i < mappingList.getLength(); ++i) {
				Node item = mappingList.item(i);
				doParseLogMapping(mapping, item);
			}
		}

	}

	public boolean getBool(String name, boolean b) {
		String a = get(name, b ? "true" : "false").toLowerCase();
		return a.equals("true");
	}

	public GeneratedMessage.Builder newLogBuilder() {
		if (getLogType().equals("flow")) {
			return FlowStarLog.newBuilder();
		} else {
			return BusinessStarLog.newBuilder();
		}

	}
	
	public Descriptor getDescriptor() {
		if (getLogType().equals("flow")) {
			return FlowStarLog.getDescriptor();
		} else {
			return BusinessStarLog.getDescriptor();
		}
	}
}
