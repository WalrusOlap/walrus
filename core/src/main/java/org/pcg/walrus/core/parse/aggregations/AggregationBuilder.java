package org.pcg.walrus.core.parse.aggregations;

import com.google.common.base.Joiner;
import org.pcg.walrus.core.CoreConstants;
import org.pcg.walrus.core.parse.aggregations.metrics.MetricFunction;
import org.pcg.walrus.core.plan.node.ASTNode;
import org.pcg.walrus.core.plan.operator.Aggregator;
import org.pcg.walrus.core.query.RColumn;
import org.pcg.walrus.core.query.RMetric;
import org.pcg.walrus.core.query.RQuery;
import org.pcg.walrus.meta.pb.WFieldMessage;

import com.google.common.reflect.ClassPath;
import com.google.common.collect.ImmutableSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AggregationBuilder {

    private static final Logger log = LoggerFactory.getLogger(AggregationBuilder.class);

    private static final String METRIC_FUNCTION_PATH = "org.pcg.walrus.core.parse.aggregations.metrics";

    /**
     * build final aggregation
     */
    public static Aggregation buildAggregation(RQuery query, Map<String, WFieldMessage.WDimension> dims,
           Map<String, WFieldMessage.WMetric> metrics) throws Exception {
        // build root aggregator
        Aggregator aggregator = new Aggregator(query.getBday(), query.getEday());
        RMetric[] ms = new RMetric[query.getMs().length];
        int index = 0;
        for(RMetric m: query.getMs()) {
            WFieldMessage.WMetric metric = metrics.get(m.getName());
            m.setMethod(metric.getMethod());
            RMetric r = new RMetric(m.getName());
            r.setMethod(metric.getMethod());
            // set metric param
            if(metric.getDerivedLogic() != null) {
                List<String> params = metric.getDerivedLogic().getParamsList();
                r.setParam(Joiner.on(",").join(params));
            }
            ms[index++] = r;
        }
        RColumn[] cols = new RColumn[query.getCols().length];
        index = 0;
        for(RColumn col: query.getCols()) {
            RColumn c = new RColumn(col.getName());
            c.setExType(CoreConstants.DERIVE_MODE_SELECT);
            cols[index++] = c;
        }
        aggregator.setCols(cols);
        aggregator.setMs(ms);
        aggregator.setLimit(query.getLimit());
        aggregator.setOrderBy(query.getOrderBy());
        ASTNode root = new ASTNode(aggregator);
        // apply aggregator metric functions
        Map<String, MetricFunction> functions = loadFunctions();
        // apply query functions
        applyFunctions(root, query, functions, query.getMs(), false);
        // set aggregator functions
        applyFunctions(root, query, functions, aggregator.getMs(), true);
        return new Aggregation(root);
    }

    // apply functions
    private static void applyFunctions(ASTNode root, RQuery query, Map<String, MetricFunction>functions,
                               RMetric[] ms, boolean apply) {
        for(RMetric m: ms) {
            MetricFunction func = functions.get(m.getMethod().toLowerCase());
            log.info(String.format("%s apply function: %s", m.getMethod(), func));
            if(func != null) {
                m.setAggregateFuntion(func);
                if(apply) func.apply(root, query, m);
            }
            // else throw new WCoreException(String.format("aggregate function %s is unsupported!", m.getMethod()));
        }
    }

    // load all metric functions[in tomcat, app class loader not working]
    private static Map<String, MetricFunction> loadFunctions() throws Exception {
        Map<String, MetricFunction> functions = new HashMap<>();
        final ClassLoader loader = AggregationBuilder.class.getClassLoader();
        ImmutableSet<ClassPath.ClassInfo> clazz = ClassPath.from(loader).getTopLevelClasses(METRIC_FUNCTION_PATH);
        for (final ClassPath.ClassInfo c : clazz) {
            final Class<?> func =  c.load();
            if(!func.isInterface()) {
                MetricFunction f = (MetricFunction) Class.forName(func.getName()).newInstance();
                functions.put(f.name().toLowerCase(), f);
            }
        }
        return functions;
    }

}
