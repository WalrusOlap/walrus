package org.pcg.walrus.core.plan.node;

/**
 * <code>VisitCallback</code> </br>
 * callback function for visiting a <CODE>ASTNode</CODE>
 */
public interface VisitCallback<T extends VisitRet> {

	public void callback(T t1, T t2);
}
