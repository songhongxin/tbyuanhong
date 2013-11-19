package com.etao.lz.star.spout;

import java.util.Map;
import java.util.Stack;



public abstract class FieldProcessor{
	private Map<String, FieldProcessor> mapping;
	public FieldProcessor(Map<String, FieldProcessor> mapping)
	{
		this.mapping = mapping;
	}
	
	protected final Object evalParam(String field, Stack<String> params) throws LogFormatException
	{
		if (params.empty()) {
			throw new IllegalStateException("Missing command for field "
					+ field);
		}
		String cmd = params.pop();
		if (!cmd.startsWith("@")) {
			return cmd;
		}
		FieldProcessor processor = mapping.get(cmd);
		if (processor == null) {
			throw new IllegalStateException("Unrecognized command " + cmd);
		}
		return processor.process(field, params);
	}
	public abstract Object process(String field, Stack<String> params) throws LogFormatException;
}
