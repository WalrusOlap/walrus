package org.pcg.walrus.core.parse.aggregations.metrics;

import org.pcg.walrus.core.CoreConstants;
import org.pcg.walrus.core.plan.OperatorVisitor;
import org.pcg.walrus.core.plan.node.ASTNode;
import org.pcg.walrus.core.plan.operator.Aggregator;
import org.pcg.walrus.core.plan.operator.Combiner;
import org.pcg.walrus.core.plan.operator.Selector;
import org.pcg.walrus.core.query.RColumn;
import org.pcg.walrus.core.query.RMetric;
import org.pcg.walrus.core.query.RQuery;

/**
 * split state to two sub stage [select ...,count(distinct col_1) uv from t_1 group by xxx]:
 *  1 add col_1 to group by columns[select ..., col_1 from t_1 group by ...,col_1]
 *  2 count col_1 as uv[select ..., COUNT(col_1) uv from t_1 group by ...]
 */
public class CountDistinct implements MetricFunction {

    @Override
    public String name() {
        return CoreConstants.METRIC_FUNCTION_DISTINCT;
    }

    /**
     * query: remove uv from metric, add ${PARAM} to columns
     * aggregator: add uv to metric
     */
    @Override
    public void apply(ASTNode root, RQuery query, RMetric metric) {
        // aggregator update distinct metric
        Aggregator aggregator = (Aggregator) root.getOperator();
        String distinctCol = metric.getParam();
        // remove uv
        query.removeMetric(metric.getName());
        // add ${PARAM}
        query.addColumn(distinctCol);
        // if query's metrics is empty, add default count
        if(query.getMs().length == 0) {
            RMetric defaultCount = new RMetric(CoreConstants.QUERY_METRIC_DEFAULT_COUNT, CoreConstants.COLUMN_TYPE_INT);
            defaultCount.setExType(CoreConstants.DERIVE_MODE_VIRTUAL);
            defaultCount.setMethod(CoreConstants.METRIC_FUNCTION_COUNT);
            query.addMetric(defaultCount);
        }
        // add sub aggregation stage if necessary
        if(root.childNum() == 0) {
            // combine all sub queries
            Combiner combiner = new Combiner(aggregator.getBday(), aggregator.getEday());
            combiner.setCols(query.getCols());
            combiner.setMs(query.getMs());
            root.appendChild(new ASTNode(combiner));
        } else { // update combiner
            root.traverseDown(new OperatorVisitor() {
                @Override
                public void visitSelector(Selector selector) {
                    // do nothing
                }
                @Override
                public void visitCombiner(Combiner combiner) {
                    combiner.setMs(RMetric.removeMetric(combiner.getMs(), metric));
                    combiner.setCols(RColumn.addColumn(combiner.getCols(), new RColumn(distinctCol)));
                }
            });
        }
    }

    // group by {param} first, just count(param) as uv
    @Override
    public String aggregateSql(String params, String alias) {
        return String.format("%s(%s) AS %s", CoreConstants.METRIC_FUNCTION_COUNT, params, alias);
    }

    @Override
    public String selectSql(String method, String params, String alias) {
        return String.format("%s(%s) AS %s", method, params, alias);
    }
}
