package org.pcg.walrus.core.execution.job;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.pcg.walrus.core.plan.node.VisitRet;
import org.pcg.walrus.common.exception.WCoreException;
import org.pcg.walrus.meta.pb.WJobMessage;

import java.io.Serializable;

/**
 * execution stage tree
 */
public interface RStage extends Serializable, VisitRet {

    // add child
    public void addStage(RStage stage);

    // execute job and return a dataframe
    public Dataset<Row> exe(WJobMessage.WJob job) throws WCoreException;

}
