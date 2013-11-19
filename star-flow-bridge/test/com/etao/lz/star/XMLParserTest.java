package com.etao.lz.star;

import junit.framework.Assert;

import org.junit.Test;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class XMLParserTest {

	@Test
	public void test() throws Exception {
		XMLParser parser = new XMLParser("/test_parser.xml");
		Assert.assertEquals(parser.getString(null, "/BoltConfig/LogBuilder"),"flow");
		NodeList nodeList = parser.getNodeList(null, "/BoltConfig/LogOutput");
		Assert.assertEquals(1, nodeList.getLength());
		for(int i=0;i<nodeList.getLength();++i)
		{
			Node node = nodeList.item(i);
			Object object = parser.getClassInstance(node, "@class");
			Assert.assertEquals(object.getClass().getSimpleName(),"WriteToHBaseProcessor");
			Assert.assertEquals(parser.getString(node, "Output"),"abc");
		}
	}

}
