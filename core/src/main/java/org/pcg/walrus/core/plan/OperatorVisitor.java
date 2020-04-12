package org.pcg.walrus.core.plan;

import org.pcg.walrus.core.CoreConstants;
import org.pcg.walrus.core.plan.node.ASTNode;
import org.pcg.walrus.core.plan.node.ASTVisitor;
import org.pcg.walrus.core.plan.operator.Combiner;
import org.pcg.walrus.core.plan.operator.Operator;
import org.pcg.walrus.core.plan.operator.Selector;

/**
 * query plan tree selector node visitor
 */
public abstract class OperatorVisitor implements ASTVisitor {

    @Override
    public void visit(ASTNode node) {
        Operator op = node.getOperator();
        if (CoreConstants.AST_OPER_SELECT.equals(op.getOperatorType())) {
            Selector selector = (Selector) node.getOperator();
            visitSelector(selector);
        }
        else if (CoreConstants.AST_OPER_COMBINER.equals(op.getOperatorType())) {
            Combiner combiner = (Combiner) node.getOperator();
            visitCombiner(combiner);
        }
    }

    public abstract void visitSelector(Selector selector);
    public abstract void visitCombiner(Combiner combiner);
}