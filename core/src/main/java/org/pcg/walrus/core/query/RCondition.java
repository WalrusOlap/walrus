package org.pcg.walrus.core.query;

import org.pcg.walrus.common.sql.DruidAstVisitor;
import org.pcg.walrus.common.sql.SqlUtil;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Set;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.*;

/**
* <code>RCondition</code> 
* walrus query condition </br>
* 
* root</br>
*   -- AND</br>
*   -- AND</br>
*      -- OR</br>
*         -- AND</br>
*      -- OR</br>
*/
public class RCondition implements Serializable {

	private static final long serialVersionUID = 1854903370741024166L;

	private String filter;
	private transient SQLExpr expr; // condition tree
	private Set<RField> columns;

	public RCondition(Set<RField> columns, String filter) {
		this.columns = columns;
		this.filter = filter;
	}

	// if condition is empty
	public boolean isEmpty() {
		return columns == null || columns.isEmpty();
	}
	
	/**
	 * deep copy <br>
	 * !be careful, this is not a full deep copy!
	 */
	public RCondition copy() {
		RCondition condition = new RCondition(columns, filter);
		if(expr != null) {
			SQLExpr exp = SqlUtil.toSqlExpr(getWhere());
			condition.setExpr(exp);
		}
		return condition;
	}

	/**
	 * add condition
	 * e.g 1=1 => (1=1) and condition
	 */
	public void addCondition(String condition) {
		SQLExpr where = SQLUtils.toMySqlExpr(condition);
		if(expr == null) expr = where;
		else expr = SQLUtils.buildCondition(SQLBinaryOperator.BooleanAnd, expr, true, where);
	}
	
	/**
	 * if simple expr match condition <br>
	 * e.g expr(is_live=1 and adx=2) , condition(adx=2) => true  <br>
	 * e.g expr(is_live=1 or adx=2) , condition(adx=2) => false  <br>
	 */
	public boolean match(String condition) {
		SQLExpr where = null;
		try { 
			where = SQLUtils.toMySqlExpr(condition);
		} catch (Exception e) {
			// do nothing
		}
		if(expr == null || where == null ) return false;
		else {
			Matcher visitor = new Matcher(where);
			expr.accept(visitor);
			return visitor.isMatch();
		}
	}
	
	/* 
	 * add alias to column
	 */
	public void replaceColumn(String column, String replacement) {
		if(expr == null) return;
		AliasAdder aliasAdder = new AliasAdder(column, replacement);
		expr.accept(aliasAdder);
	}

	/**
	 *  expr to sql
	 */
	public String getWhere() {
		if(expr == null) return "1=1";
		else return SQLUtils.toSQLString(expr).replace("\n", " ").replace(	"\t", "");
	}

	@Override
	public boolean equals(Object obj) {
        if (obj instanceof RCondition) {   
        	RCondition r = (RCondition) obj;   
        	return getWhere().equals(r.getWhere());
        }
        return super.equals(obj);
	}

	public Set<RField> getColumns() {
		return columns;
	};

	public void setColumns(Set<RField> columns) {
		this.columns = columns;
	}

	public SQLExpr getExpr() {
		return expr;
	}

	public void setExpr(SQLExpr expr) {
		this.expr = expr;
	}

	/**
	 * to serializable expr
	 * @param stream
	 * @throws IOException
	 */
	private void writeObject(ObjectOutputStream stream) throws IOException {
		stream.defaultWriteObject();
		stream.writeObject(expr.toString());
	}

	/**
	 * to deserializable expr
	 * @param stream
	 * @throws IOException
	 */
	private void readObject(ObjectInputStream stream) throws IOException,
			ClassNotFoundException {
		stream.defaultReadObject();
		String exprStr = (String) stream.readObject();
		expr = SqlUtil.toSqlExpr(exprStr);
	}

	@Override
	public String toString() {
		return columns + "[" + getWhere() + "]";
	}
}

/**
 * visit condition tree and judge if sql expr match condition:
 *	 e.g expr(is_live=1 and adx=2) , condition(adx=2) => true  <br>
 *   e.g expr(is_live=1 or adx=2) , condition(adx=2) => false  <br>
 *
 *  ! Be careful, this function is not Strictly correct， only for simple cases like view ignore
 */
class Matcher extends DruidAstVisitor {
	private SQLExpr expr;
	private boolean match = false;
	private boolean hasOr = false;

	public Matcher(SQLExpr expr) {
		this.expr = expr;
	}

	public boolean isMatch() {
		return match;
	}

	@Override
	protected void visitBinaryLeaf(SQLBinaryOpExpr expr) {
		if(expr.getParent() instanceof SQLBinaryOpExpr)
			hasOr = hasOr || SQLBinaryOperator.BooleanOr.equals(((SQLBinaryOpExpr) expr.getParent()).getOperator());
		match = !hasOr & (match
				|| this.expr.toString().replace("'", "").equals(expr.toString().replace("'", "")));
	}

	@Override
	protected void visitIn(SQLInListExpr expr) {}

	@Override
	protected void visitBetween(SQLBetweenExpr expr) {}

}

/**
 * visit condition tree and add alias to column
 */
class AliasAdder extends DruidAstVisitor {
	private String col;
	private String replacement;
	public AliasAdder(String col, String replacement) {
		this.col = col;
		this.replacement = replacement;
	}

	@Override
	protected void visitBetween(SQLBetweenExpr expr) {}

	@Override
	protected void visitBinaryLeaf(SQLBinaryOpExpr expr) {
		SQLExpr left = replaceExpr(expr.getLeft());
		if(left != null) expr.setLeft(left);
		SQLExpr right = replaceExpr(expr.getRight());
		if(right != null) expr.setRight(right);
	}

	/*
	 * in expr: col_1 in () -> alias.col_1 in ()
	 */
	@Override
	protected void visitIn(SQLInListExpr x) {
		SQLExpr expr = replaceExpr(x.getExpr());
		if(expr != null) x.setExpr(expr);
	}

	/*
	 * condition
	 * 1 a expr b
	 * 2 func(a) expr b
	 */
	private SQLExpr replaceExpr(SQLExpr x) {
		String column = x.toString();
		String func = "(" + col + ")";
		if(col.equals(column)) return SQLUtils.toMySqlExpr(replacement); // a expr b
		else if(column.contains(func)) return SQLUtils.toMySqlExpr(column.replace(col, replacement)); // func(a) expr b
		else return null;
	}
}