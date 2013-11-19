package com.etao.lz.star.spout;

import java.io.Serializable;
import java.util.Stack;

public class LogMappingItem implements Serializable {
	private static final long serialVersionUID = 151520481088242322L;
	private String name;
	/**
	 * commands contains a list of commands and values. each string starts with @
	 * is a command which take one or more arguments. <br>
	 * For example: "@ts", "@field", "10" will be evaluated to @ts(@field(10))<br>
	 * The semantic of each command is defined in FieldMappingManager<br>
	 */
	private String[] commands;

	public LogMappingItem(String name, String commands) {
		this.name = name;
		this.commands = commands.split("\\s+");
	}

	public String getName() {
		return name;
	}

	public void fillCommandStack(Stack<String> stack) {
		stack.clear();
		// push command to stack in reverse order,
		// so that it can be popped in normal order 
		for (int i = commands.length; i > 0; i--) {
			stack.push(commands[i - 1]);
		}
	}
}
