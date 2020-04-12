package org.pcg.walrus.core.plan.node;

import java.io.Serializable;

/**
 * walrus query plan, one <CODE>ASTNode</CODE> for each cluster
 */
public class ASTree implements Serializable {

	private static final long serialVersionUID = -8864267414981435610L;

	private ASTNode root;

	public ASTree(ASTNode root) {
		this.root = root;
	}

	/**
	 * traverse tree with <CODE>ASTVisitor</CODE>
	 */
	public void traverse(ASTVisitor visitor) {
		root.traverseDown(visitor);
	}

	/**
	 * traverse tree with <CODE>ASTVisitor1</CODE>
	 */
	public VisitRet traverse(ASTVisitor1<VisitRet> visitor,
			VisitCallback<VisitRet> callback, VisitRet parent) {
		return root.traverseDown(visitor, callback, parent);
	}

	public ASTNode getRoot() {
		return root;
	}

	@Override
	public String toString() {
		return root.toString();
	}

}
