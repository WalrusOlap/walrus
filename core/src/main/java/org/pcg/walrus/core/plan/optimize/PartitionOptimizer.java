package org.pcg.walrus.core.plan.optimize;

import org.pcg.walrus.core.plan.OperatorVisitor;
import org.pcg.walrus.core.plan.node.ASTree;
import org.pcg.walrus.core.plan.operator.Combiner;
import org.pcg.walrus.core.plan.operator.Selector;
import org.pcg.walrus.core.query.RCondition;
import org.pcg.walrus.core.query.RPartition;
import org.pcg.walrus.common.env.WContext;
import org.pcg.walrus.meta.MetaConstants;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;

/**
 * if too many stage in one selector, union them <br>
 */
public class PartitionOptimizer extends OperatorVisitor implements IOpitimizer  {

    private WContext context;
    private static final int MAX_PARTITION_NUM = 10;

    @Override
    public void optimize(WContext context, ASTree tree) {
        this.context = context;
        tree.getRoot().traverseDown(this);
    }

    @Override
    public void visitSelector(Selector selector) {
        List<RPartition> partitions = selector.getPartitions();
        boolean same = true;
        RCondition r = null;
        // if all partition has same condition
        for(RPartition p: partitions) {
            r = r == null ? p.getCondition() : r;
            same = same && r.equals(p.getCondition());
        }
        if(same && partitions.size() > MAX_PARTITION_NUM) {
            List<RPartition> ps = new ArrayList<RPartition>();
            List<List<RPartition>> lists = Lists.partition(partitions, MAX_PARTITION_NUM);
            for(List<RPartition> list: lists) {
                RPartition partition = null;
                Dataset<Row> df = null;
                for(RPartition p: list) {
                    if(partition == null) partition = p.copy();
                    if(p.getBday().before(partition.getBday())) partition.setBday(p.getBday());
                    if(p.getEday().after(partition.getEday())) partition.setEday(p.getEday());
                    String tname = p.getTname();
                    df = df == null ? context.getSqlContext().table(tname) : df.union(context.getSqlContext().table(tname));
                }
                String table = partition.getTname().split(MetaConstants.META_DELIM_NAME)[0]
                        + MetaConstants.META_DELIM_NAME + partition.getBday()
                        + MetaConstants.META_DELIM_NAME + partition.getEday();
                context.getSqlContext().registerDataFrameAsTable(df, table);
                partition.setTname(table);
                ps.add(partition);
            }
            selector.setPartitions(ps);
        }
    }

    @Override
    public void visitCombiner(Combiner combiner) {
        // do nothing
    }

    @Override
    public boolean apply() {
        return true; // always apply
    }

    @Override
    public String name() {
        return "partition_union_optimizer";
    }
}
