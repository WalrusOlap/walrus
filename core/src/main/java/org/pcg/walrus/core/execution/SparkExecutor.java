package org.pcg.walrus.core.execution;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.storage.StorageLevel;
import org.pcg.walrus.core.CoreConstants;
import org.pcg.walrus.core.execution.job.RStage;
import org.pcg.walrus.core.execution.job.stage.CkStage;
import org.pcg.walrus.core.execution.job.RJob;
import org.pcg.walrus.core.execution.job.stage.KylinStage;
import org.pcg.walrus.core.execution.job.stage.SqlStage;
import org.pcg.walrus.core.execution.job.stage.SparkStage;
import org.pcg.walrus.core.execution.sink.DbSinker;
import org.pcg.walrus.core.execution.sink.HdfsSinker;
import org.pcg.walrus.core.execution.sink.ISinker;
import org.pcg.walrus.core.execution.sink.JdbcSinker;
import org.pcg.walrus.core.plan.LogicalPlan;
import org.pcg.walrus.core.plan.node.ASTNode;
import org.pcg.walrus.core.plan.node.ASTVisitor1;
import org.pcg.walrus.core.plan.node.ASTree;
import org.pcg.walrus.core.plan.node.VisitCallback;
import org.pcg.walrus.core.plan.operator.*;
import org.pcg.walrus.core.query.RPartition;

import org.pcg.walrus.common.env.WContext;
import org.pcg.walrus.common.exception.WCoreException;
import org.pcg.walrus.meta.MetaConstants;
import org.pcg.walrus.common.util.LogUtil;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import org.pcg.walrus.meta.pb.WJobMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * spark execution engine
 */
public class SparkExecutor implements IExecutor {

	private static final Logger log = LoggerFactory.getLogger(SparkExecutor.class);

	private static final int DB_MAX_SIZE = 1000000; // max data size to save into db

	@Override
	public WResult execute(WContext context, LogicalPlan plan, WJobMessage.WJob wJob) throws WCoreException {
		ASTree tree = plan.getPlan();
		RJob job = buildJob(context, tree);
		LogUtil.info(log, wJob.getId(), "RJob: " + job);
		Dataset<Row> dataframe = job.exe(wJob);
		// fill null with 未知
		dataframe = dataframe.na().fill("未知");
		LogUtil.info(log, wJob.getId(), "dataset: " + dataframe);
		dataframe.persist(StorageLevel.MEMORY_AND_DISK_SER());
		LogUtil.info(log, wJob.getId(), "Final df: " + dataframe);
		long lines = dataframe.count();
		// set return
		WResult result = new WResult();
		result.setLines(lines);
		// sink result
		String saveMode = StringUtils.isBlank(wJob.getSaveMode()) ? CoreConstants.SAVE_MODE_HDFS : wJob.getSaveMode();
		result.setSaveMode(saveMode);

		// sink
		ISinker sinker = null;
		switch (saveMode.toLowerCase()) {
			case CoreConstants.SAVE_MODE_DB:
				// if data size is more than {DB_MAX_SIZE}, save to hdfs
				sinker = new JdbcSinker();
				break;
			case CoreConstants.SAVE_MODE_MYSQL:
			case CoreConstants.SAVE_MODE_CK:
				// if data size is more than {DB_MAX_SIZE}, save to hdfs
				sinker = new DbSinker();
				break;
			case CoreConstants.SAVE_MODE_HDFS:
			default:
				sinker = new HdfsSinker();
		}
		// if data size is more than {DB_MAX_SIZE}, save to hdfs
		if(lines > DB_MAX_SIZE) {
			result.setSaveMode(CoreConstants.SAVE_MODE_HDFS);
			sinker = new HdfsSinker();
		}
		sinker.sink(context, dataframe, wJob, result);
		return result;
	}

	// visit logic plan tree and build a <CODE>RJob</CODE>
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private RJob buildJob(WContext context, ASTree tree) {
		ASTVisitor1 visitor = new StageVisitor(context);
		VisitCallback callback = new StageCallback();
		RStage stage = (RStage) tree.traverse(visitor, callback, null);
		return new RJob(stage);
	}

	// visit <CODE>ASTNode</CODE> and build a <CODE>EStage</CODE>
	private class StageVisitor implements ASTVisitor1<RStage> {
		private WContext context;

		public StageVisitor(WContext context) {
			this.context = context;
		}

		@Override
		public RStage visit(ASTNode node) {
			RStage stage = null;
			Operator operator = node.getOperator();
			switch (operator.getOperatorType()) {
			case CoreConstants.AST_OPER_AGGREGATOR:
				// final AGGREGATION
				Aggregator aggregator = (Aggregator) operator;
				stage = new SparkStage(context, null, aggregator);
				break;
			case CoreConstants.AST_OPER_UNION:
				// union
				Unifier union = (Unifier) operator;
				stage = new SparkStage(context, null, union);
				break;
			case CoreConstants.AST_OPER_COMBINER:
				// union
				Combiner combiner = (Combiner) operator;
				stage = new SparkStage(context, null, combiner);
				break;
			case CoreConstants.AST_OPER_SELECT:
				// selector
				Selector selector = (Selector) operator;
				stage = new SparkStage(context, null, new Unifier(CoreConstants.OPER_UNION_SIMPLE, null));
				for (RPartition partition : selector.getPartitions()) {
					SqlStage s = null;
					switch (partition.getFormat()) {
					// druid selector
					case MetaConstants.META_FORMAT_DRUID:
						// TODO
						break;
					case MetaConstants.META_FORMAT_KUDU:
						// TODO
						break;
					case MetaConstants.META_FORMAT_ES:
						// TODO
						break;
					case MetaConstants.META_FORMAT_KYLIN:
						s = new KylinStage(context, partition, selector);
						break;
					case MetaConstants.META_FORMAT_CK:
						s = new CkStage(context, partition, selector);
						break;
					// spark sql selector
					case MetaConstants.META_FORMAT_PARQUET:
					case MetaConstants.META_FORMAT_JSON:
					case MetaConstants.META_FORMAT_ORC:
					case MetaConstants.META_FORMAT_JDBC:
					case MetaConstants.META_FORMAT_CSV:
					default:
						s = new SparkStage(context, partition, selector);
						break;
					}
					// add stage for each partition
					stage.addStage(s);
				}
				break;
			default:
				break;
			}
			return stage;
		}
	}

	// child stage callback
	private class StageCallback implements VisitCallback<SqlStage> {
		@Override
		public void callback(SqlStage t1, SqlStage t2) {
			t1.addStage(t2);
		}
	}

}
