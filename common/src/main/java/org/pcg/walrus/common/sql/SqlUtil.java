package org.pcg.walrus.common.sql;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.util.JdbcConstants;

import java.util.List;

public class SqlUtil {

    public static SQLExpr toSqlExpr(String exprStr) {
        String sql = "SELECT COL_A FROM TABLE_A WHERE " + exprStr;
        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql,
                JdbcConstants.MYSQL);
        SQLSelectStatement stmt = (SQLSelectStatement) stmtList.get(0);
        SQLSelect select = stmt.getSelect();
        SQLSelectQueryBlock block = (SQLSelectQueryBlock) select.getQuery();
        return block.getWhere();
    }
}
