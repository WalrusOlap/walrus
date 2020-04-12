package org.pcg.walrus.core.execution.job;

import java.io.Serializable;

import org.pcg.walrus.common.exception.WCoreException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.pcg.walrus.meta.pb.WJobMessage;

/**
* <code>EJob</code> </br>
* execution job tree
*/ 
public class RJob implements Serializable {

	private static final long serialVersionUID = 4368191801080676503L;

	private RStage root;

	public RJob(RStage root) {
		this.root = root;
	}
	
	// execute job
	public Dataset<Row> exe(WJobMessage.WJob job) throws WCoreException {
		return root.exe(job);
	}
	
	@Override
	public String toString() {
		return root.toString();
	}
}
