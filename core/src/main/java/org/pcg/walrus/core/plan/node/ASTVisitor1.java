package org.pcg.walrus.core.plan.node;

/**
 * <code>ASTVisitor1</code> </br>
 * visit function for <CODE>ASTNode</CODE>
 */
public interface ASTVisitor1<R extends VisitRet> {

	public R visit(ASTNode node); 

}
