package org.pcg.walrus.core.plan.optimize;

import com.google.common.base.Joiner;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import org.pcg.walrus.common.env.WContext;
import org.pcg.walrus.common.util.WalrusUtil;
import org.pcg.walrus.core.CoreConstants;
import org.pcg.walrus.core.plan.OperatorVisitor;
import org.pcg.walrus.core.plan.node.ASTree;
import org.pcg.walrus.core.plan.operator.Combiner;
import org.pcg.walrus.core.plan.operator.Selector;
import org.pcg.walrus.core.query.RDict;
import org.pcg.walrus.core.query.RPartition;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 *
 * ! IMPORTANT, this Optimizer is only for left join, better way are:
 *     1 set dict isBc = 1, use map join on small dataset
 *     2 more executors, more memory
 */
public class SkewJoinOptimizer extends OperatorVisitor implements IOpitimizer  {

    private WContext context;
    private double sampleSize = 2000d;
    private double minSkewVal = 0.5;

    /**
     * 1 check if there is skew problem in each partition
     * 2 if yes, split stage to:
     *   2.1 cold stage: remove hot values
     *   2.2 hot stage: broadcast join dict
     */
    @Override
    public void visitSelector(Selector selector) {
        // register udf
        List<RPartition> partitions = new ArrayList<RPartition>();
        for(RPartition partition: selector.getPartitions()) {
            String tname = partition.getTname();
            Dataset<Row> tFact = context.getSqlContext().table(tname);
            long count = tFact.count();
            double fraction = count >= sampleSize ? sampleSize / count : 1;
            Set<RDict> dicts = partition.getDicts();
            boolean isSkew = false;
            for(RDict dict: dicts) {
                // only for left join
                if(!CoreConstants.TYPE_JOIN_LEFT.equalsIgnoreCase(dict.getDict().getJoinType())) break;
                String dname = dict.getTable();
                String[] parentKeys = dict.getFactKeysList();
                String[] dictKeys = dict.getDictKeysList();
                for(int i = 0; i < parentKeys.length; i++) {
                    String factKey = parentKeys[i];
                    String dictKey = dictKeys[i];
                    // lower skewness means skewness problem
                    double skewness = WalrusUtil.skewness(tFact, factKey, fraction);
                    if(skewness < minSkewVal) {
                        isSkew = true;
                        // get hot values
                        String[] hotVals = WalrusUtil.hotVals(tFact, factKey, hotKeyNum(), fraction);
                        String hotVal = String.format("'%s'", Joiner.on("','").join(hotVals));
                        // normal partition: remove hot values
                        RPartition coldlPartition = partition.copy();
                        coldlPartition.getCondition().addCondition(String.format("%s.%s not in (%s)",
                                CoreConstants.SQL_FACT_TABLE_ALIAS, factKey, hotVal));
                        partitions.add(coldlPartition);
                        // right partition: broadcast join dict
                        RPartition hotPartition = partition.copy();
                        hotPartition.getCondition().addCondition(String.format("%s.%s in (%s)",
                                CoreConstants.SQL_FACT_TABLE_ALIAS, factKey, hotVal));
                        Dataset<Row> tDict = context.getSqlContext().table(dname);
                        String bDict = String.format("%s_broadcast", dname);
                        WalrusUtil.cacheTable(context.getSqlContext(), tDict.filter(String.format("%s in (%s)", dictKey, hotVal)), bDict);
                        hotPartition.updateDictTable(dict.getDictName(), bDict);
                        partitions.add(hotPartition);
                        // break, only handle one key is enough ?
                        break;
                    }
                }
            }
            // if not skew, add origin partition
            if(!isSkew) partitions.add(partition);
        }
        // update partitions
        selector.setPartitions(partitions);
    }

    @Override
    public void visitCombiner(Combiner combiner) {}

    @Override
    public void optimize(WContext context, ASTree tree) {
        this.context = context;
        tree.getRoot().traverseDown(this);
    }

    // top hot keys
    private int hotKeyNum() {
        String num = System.getProperty("walrus.optimizer.skew.join.hot.keys", "10");
        return Integer.parseInt(num);
    }

    /**
     * if need apply skewJoinOptimizer, run task with parameter "--conf spark.driver.extraJavaOptions=-Dwalrus.optimizer.skew.join=true"
     */
    @Override
    public boolean apply() {
        String skew = System.getProperty("walrus.optimizer.skew.join", "false");
        return "true".equalsIgnoreCase(skew);
    }

    @Override
    public String name() {
        return "data_skew_join_optimizer";
    }
}
