package org.pcg.walrus.core.execution.sink;

import org.pcg.walrus.common.env.WContext;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.pcg.walrus.common.exception.WCoreException;
import org.pcg.walrus.core.execution.WResult;
import org.pcg.walrus.meta.pb.WJobMessage;

public interface ISinker {

    public void sink(WContext context, Dataset<Row> df, WJobMessage.WJob job, WResult result) throws WCoreException;
}
