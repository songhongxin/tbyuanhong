package com.etao.lz.star.spout;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LogMapping implements Serializable{

	private static final long serialVersionUID = 3810512692228827746L;
	private Map<String, List<LogMappingItem>> mapping;

	public LogMapping() {
		mapping = new HashMap<String, List<LogMappingItem>>();
	}

	public LogMapping(LogMapping logMappings) {
		this.mapping = logMappings.mapping;
	}

	public List<LogMappingItem> getMapping(String mappingName) {
		return mapping.get(mappingName);
	}

	public void put(String name, List<LogMappingItem> value) {
		mapping.put(name, value);
		
	}

	public Collection<String> getMapNames() {
		return mapping.keySet();
	}

}
