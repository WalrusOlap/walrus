package org.pcg.walrus.core.plan.node;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.pcg.walrus.core.plan.operator.Operator;

/**
 * ASTNode
 */
public class ASTNode implements Serializable {

	private static final long serialVersionUID = -4680647768922121104L;

	private ASTNode parent; // parent
	private List<ASTNode> children; // children

	private Operator operator;

	public ASTNode(Operator operator) {
		this.operator = operator;
		children = new ArrayList<ASTNode>();
	}

	public void setOperator(Operator operator) {
		this.operator = operator;
	}

	/**
	 * append new node
	 */
    public void appendChild(ASTNode node) {
        children.add(node);
		node.parent = this;
    }

    /**
	 * children num
	 */
    public int childNum() {
        return children.size();
    }
 
    /**
     * node depth
     */
    public int getDepth() {
        int d = 0;
        ASTNode node = this;
        while ((node = node.parent) != null) d++;
        return d;
    }
    
    /**
     * recursive traverse node and parent
     */
    public void traverseUp(ASTVisitor visitor) {
    	if (visitor != null) visitor.visit(this);
        if(parent == null) return;
        // visit parent
        parent.traverseUp(visitor);
    }

    /**
     * recursive traverse node and children
     */
    public void traverseDown(ASTVisitor visitor) {
        if (visitor != null) visitor.visit(this);
        if(children == null || children.isEmpty()) return;
        // visit children
        for(ASTNode child: children) child.traverseDown(visitor);
    }

    /**
     * visit this node and return <CODE>VisitRet</CODE>
     */
	public VisitRet traverseDown(ASTVisitor1<VisitRet> visitor,
			VisitCallback<VisitRet> callback, VisitRet parent) {
		VisitRet ret = visitor.visit(this);
		if(parent != null) callback.callback(parent, ret);
		// visit children
        for(ASTNode child: children) child.traverseDown(visitor, callback, ret);
    	return ret;
    }
  
	public Operator getOperator() {
		return operator;
	}

	public ASTNode parent() {
		return parent;
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		String prefix = "\n";
		for (int i = 0; i < getDepth(); i++) prefix += "\t";
		sb.append(prefix + operator);
		for (ASTNode child : children) sb.append(child);
		return sb.toString();
	}
}
