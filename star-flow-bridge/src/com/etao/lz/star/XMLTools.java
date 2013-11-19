package com.etao.lz.star;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class XMLTools {
	public static void parse(Node node, Object target) throws Exception {
		Method[] methods = target.getClass().getMethods();
		Map<String, Method> methodMap = new HashMap<String, Method>();
		for (Method m : methods) {
			String mname = m.getName();
			if (!mname.startsWith("set") || mname.length() < 4)
				continue;
			if (m.getParameterTypes().length != 1)
				continue;
			mname = Character.toLowerCase(mname.charAt(3)) + mname.substring(4);
			m.setAccessible(true);
			methodMap.put(mname, m);
		}

		NodeList childNodes = node.getChildNodes();
		for (int i = 0; i < childNodes.getLength(); ++i) {
			Node item = childNodes.item(i);
			String type = item.getNodeName();
			if(item.getNodeType() != Node.ELEMENT_NODE) continue;
			
			Node namedItem = item.getAttributes().getNamedItem("name");
			Method method = methodMap.get(namedItem.getTextContent());
			if (method == null) {
				throw new IllegalStateException("No method "
						+ namedItem.getTextContent());
			}
			if (type.equals("Property")) {
				setAttribute(target, namedItem.getTextContent(), method, item);
			} else if (type.equals("Instance")) {
				Node classItem = item.getAttributes().getNamedItem("class");
				Class<?> cls = Class.forName(classItem.getTextContent().trim());
				Object inst = cls.newInstance();
				parse(item, inst);
				if (!method.getParameterTypes()[0].equals(cls)) {
					throw new IllegalStateException(
							"Wrong method, expect parameter type"
									+ cls.getSimpleName());
				}
				method.invoke(target, inst);
			}
		}
	}

	private static void setAttribute(Object target, String name, Method method,
			Node item) throws Exception {
		Node valueItem = item.getAttributes().getNamedItem("value");
		String value;
		if(valueItem == null)
		{
			value = item.getTextContent();
		}
		else
		{
			value = valueItem.getTextContent().trim();
		}
		Class<?> cls = method.getParameterTypes()[0];
		if (cls.equals(int.class)) {
			method.invoke(target, Integer.parseInt(value));
		} else if (cls.equals(float.class)) {
			method.invoke(target, Float.parseFloat(value));
		} else if (cls.equals(String.class)) {
			method.invoke(target, value);
		} else if (cls.equals(boolean.class)) {
			method.invoke(target, value.toLowerCase().equals("true"));
		} else {
			throw new IllegalStateException("Wrong parameter "
					+ cls.getSimpleName());
		}
	}
}
