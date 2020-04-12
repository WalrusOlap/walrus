package org.pcg.walrus.common.sql;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;

/**
 * druid ast visitor
 * just visit root node
 */
public abstract class DruidAstVisitor extends MySqlSchemaStatVisitor {

    @Override
    public boolean visit(SQLInListExpr x) {
        visitIn(x);
        return false;
    }

    @Override
    public boolean visit(SQLBetweenExpr x) {
        visitBetween(x);
        return false;
    }

    @Override
    public boolean visit(SQLBinaryOpExpr x) {
        visitBinary(x);
        return false;
    }

    // visit binary
    private void visitBinary(SQLBinaryOpExpr x) {
        SQLExpr left = x.getLeft();
        SQLExpr right = x.getRight();
        if(left.getChildren().size() != 0) visitLeaf(left);
        if (right.getChildren().size() != 0) visitLeaf(right);
        // if compare, visit foot
        visitBinaryLeaf(x);
    }

    // TODO support more operatores
    private void visitLeaf(SQLExpr leaf) {
        // and, or
        if(leaf instanceof SQLBinaryOpExpr) visitBinary((SQLBinaryOpExpr) leaf);
        // in
        else if(leaf instanceof SQLInListExpr) visitIn((SQLInListExpr) leaf);
        // not
        else if(leaf instanceof SQLNotExpr) visitNot((SQLNotExpr) leaf);
        // between
        else if(leaf instanceof SQLBetweenExpr) visitBetween((SQLBetweenExpr) leaf);
        else return; // do nothing
//        else throw new Exception(String.format("unsupported operator: %s", leaf.toString()));
    }

    // visit not
    private void visitNot(SQLNotExpr x) {
        SQLExpr expr = x.getExpr();
        if(expr instanceof SQLBinaryOpExpr) visitBinary((SQLBinaryOpExpr) expr);
    }

    // action on leaf
    protected abstract void visitBinaryLeaf(SQLBinaryOpExpr expr);
    protected abstract void visitIn(SQLInListExpr expr);
    protected abstract void visitBetween(SQLBetweenExpr expr);
}
