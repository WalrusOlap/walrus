package org.pcg.walrus.core.parse.aggregations;

import org.pcg.walrus.core.plan.node.ASTNode;
import org.pcg.walrus.core.plan.node.ASTVisitor;

/**
 * aggregation tree
 */
public class Aggregation {

    public ASTNode root;

    public Aggregation(ASTNode root){
        this.root = root;
    }

    public ASTNode root() {
        return root;
    }

    public ASTNode leaf() {
        LeafVisitor visitor = new LeafVisitor();
        root.traverseDown(visitor);
        return visitor.leaf;
    }

    /**
     * get left node
     */
    private class LeafVisitor implements ASTVisitor {
        private ASTNode leaf = null;

        @Override
        public void visit(ASTNode node) {
            leaf = node;
        }
    }
}
