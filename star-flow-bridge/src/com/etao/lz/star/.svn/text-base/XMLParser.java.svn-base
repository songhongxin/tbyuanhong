package com.etao.lz.star;

import java.io.InputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class XMLParser {
	private Document _doc;
	private XPath _xpath;
	private Log LOG = LogFactory.getLog(XMLParser.class);
	
	public XMLParser(String path) throws Exception
	{
		LOG.info("input xml:" + path);
		InputStream ins = XMLParser.class.getResourceAsStream(path);
		DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
		docFactory.setNamespaceAware(true); // never forget this!
		DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
		_doc = docBuilder.parse(ins);
		
		XPathFactory factory = XPathFactory.newInstance();
		_xpath = factory.newXPath();
	}
	
	public String getString(Node node, String xpathExpr) throws Exception
	{
		if(node == null) node = _doc;
		XPathExpression expr = _xpath.compile(xpathExpr);
		String value = (String) expr.evaluate(node, XPathConstants.STRING);
		if (value == null) {
			throw new IllegalStateException("missing "+xpathExpr);
		}
		return value.trim();
	}
	
	public NodeList getNodeList(Node node, String xpathExpr) throws Exception
	{
		if(node == null) node = _doc;
		XPathExpression expr = _xpath.compile(xpathExpr);
		NodeList nodes = (NodeList) expr.evaluate(node, XPathConstants.NODESET);
		if (nodes == null) {
			throw new IllegalStateException("missing "+xpathExpr);
		}
		return nodes;
	}
	public Node getNode(Node parent, String xpathExpr) throws Exception
	{
		if(parent == null) parent = _doc;
		XPathExpression expr = _xpath.compile(xpathExpr);
		Node node = (Node) expr.evaluate(parent, XPathConstants.NODE);
		if (node == null) {
			throw new IllegalStateException("missing "+xpathExpr);
		}
		return node;
	}
	public Object getClassInstance(Node node, String attr) throws Exception
	{
		String value = getString(node, attr);
		Class<?> cls = Class.forName(value);
		
		return cls.newInstance();

	}
}
