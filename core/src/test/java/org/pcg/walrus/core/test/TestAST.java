package org.pcg.walrus.core.test;

import static org.junit.Assert.assertTrue;

import java.text.ParseException;

import org.pcg.walrus.core.CoreConstants;
import org.pcg.walrus.core.plan.node.ASTNode;
import org.pcg.walrus.core.plan.node.ASTVisitor;
import org.pcg.walrus.core.plan.node.ASTree;
import org.pcg.walrus.core.plan.operator.Operator;
import org.pcg.walrus.core.plan.operator.Selector;
import org.pcg.walrus.core.plan.operator.Unifier;
import org.junit.Test;

import org.pcg.walrus.common.util.TimeUtil;

public class TestAST {

	private int visitTreeCount = 0;
	private int visitNodeCount = 0;

	public static ASTNode buildNode(ASTNode root) throws ParseException {
		Operator lo = new Selector(TimeUtil.intToDate(20170301), TimeUtil.intToDate(20170329));
		ASTNode child = new ASTNode(lo);
		root.appendChild(child);
		return child;
	}

	public static ASTree buildTree(ASTNode root) {
		return new ASTree(root);
	}
	
	@Test
	public void testTsraverseParent() throws Exception{
		Operator operator = new Unifier(CoreConstants.OPER_UNION_SIMPLE, null);
		ASTNode root = new ASTNode(operator);
		ASTNode son = buildNode(root);
		ASTNode d = buildNode(son);
		ASTNode e = buildNode(d);
		e.traverseUp(new Visitor());
		assertTrue(visitNodeCount == 4);
	}
	
	@Test
	public void testTsraverse() throws Exception{
		Operator operator = new Unifier(CoreConstants.OPER_UNION_SIMPLE, null);
		ASTNode root = new ASTNode(operator);
		ASTNode son = buildNode(root);
		ASTNode daughter = buildNode(root);
		ASTree tree = buildTree(root);
		Visitor v = new Visitor();
		tree.traverse(v);
		assertTrue(visitTreeCount == 3);
	}
	
	private class Visitor implements ASTVisitor {
		
		@Override
		public void visit(ASTNode node) {
			visitTreeCount += 1;
			visitNodeCount += 1;
			
		}
		
	}
}
