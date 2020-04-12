package org.pcg.walrus.core.parse;

import java.util.*;

import org.pcg.walrus.meta.MetaConstants;
import org.pcg.walrus.meta.MetaUtils;
import org.pcg.walrus.meta.pb.WTableMessage;
import org.pcg.walrus.meta.tree.MTableNode;
import org.pcg.walrus.meta.tree.MViewNode;

import org.pcg.walrus.common.util.CollectionUtil;
import org.pcg.walrus.common.util.LogUtil;
import org.pcg.walrus.common.util.TimeUtil;
import org.pcg.walrus.common.exception.WCoreException;

import org.pcg.walrus.core.CoreConstants;
import org.pcg.walrus.core.query.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;
import com.google.common.base.Joiner;

/**
 * Parse Validator
 */
public class ParseValidator {

	private static final Logger log = LoggerFactory.getLogger(ParseValidator.class);

	/**
	 * validate view
	 */
	protected void validateView(MViewNode view, RQuery query,
                                Map<String, MTableNode> tables, Map<String, MTableNode> dicts,
                                String cluster, long jobId) throws WCoreException {
		// check if view cover query dates
		Date bday = query.getBday();
		Date eday = query.getEday();
		Set<String> days = MetaUtils.getViewCoverDays(view.getView(), tables, cluster);
		LogUtil.info(log, jobId, "view cover days: " + days.size());
		Set<String> calDays = Sets.newHashSet();
		for(Date i: TimeUtil.getPartitonDays(bday, eday)) calDays.add(TimeUtil.DateToString(i));
		Set<String> ds = CollectionUtil.diffSets(days, calDays);
		if(ds.size() > 0)
			throw new WCoreException("unsupported calculate days: " + ds);
		// check if view cover query fields
		Set<String> dims = MetaUtils.getViewDims(view.getView(), tables, dicts, bday, eday, cluster).keySet();
		Set<String> cols = query.realColumns();
		LogUtil.info(log, jobId, "view dims: " + dims);
		LogUtil.info(log, jobId, "query dims: " + cols);
		Set<String> scols = CollectionUtil.diffSets(dims, cols);
		if(!scols.isEmpty()) throw new WCoreException("unknown columns: (" + scols.size() + ")" + scols);
		Set<String> metrics = MetaUtils.getViewMetrics(view.getView(),
				tables, dicts, bday, eday, cluster).keySet();
		LogUtil.info(log, jobId, "view metrics: " + metrics);
		Set<String> sms = CollectionUtil.diffSets(metrics, query.realMetrics());
		LogUtil.info(log, jobId, "query metrics: " + query.realMetrics());
		if(sms.size() > 0) throw new WCoreException("unknown metrics: " + sms);
	}

	/**
	 * validate table
	 */
	protected void validateTable(RTable table, RQuery query, long jobId) throws WCoreException {
		List<String> errors = new ArrayList<>();
		Date bday = query.getBday();
		Date eday = query.getEday();
		String level = query.getLevel();
		// check query table
		List<String> levelList = table.getTableNode().getTable().getLevelList();
		// check cluster
		if(!levelList.contains(level))
			errors.add("meta error: unknown cluster[" + level + "] on table: " + query.getTname());
		// check cover days
		Set<String> days = table.getTableNode().coverDays(level);
		LogUtil.info(log, jobId, "table cover days: " + days);
		Set<String> calDays = Sets.newHashSet();
		for(Date i: TimeUtil.getPartitonDays(bday, eday)) calDays.add(TimeUtil.DateToString(i));
		Set<String> ds = CollectionUtil.diffSets(days, calDays);
		LogUtil.debug(log, jobId, "calDays: " + calDays);
		if(ds.size() > 0)
			errors.add("unsupported calculate days: " + ds);
		// check columns
		Set<String> cs = table.columns(query, true);
		if(cs.isEmpty())
			errors.add("data not ready: " + eday);
		Set<String> cols = query.fields();
		Set<String> diffCols = CollectionUtil.diffSets(cs, cols);
		if(!diffCols.isEmpty())
			errors.add("unknown columns: " + diffCols);
		// if data stored in mysql|ck|kylin..., join is not supported:
		// 	1 join column is not supported
		// 	2 join metric(which in base table) is not supported
		String[] formats = new String[]{
				MetaConstants.META_FORMAT_JDBC,
				MetaConstants.META_FORMAT_KUDU,
				MetaConstants.META_FORMAT_KYLIN,
				MetaConstants.META_FORMAT_CK
		};
		WTableMessage.WTablet tablet = table.getTableNode().getPartitions().values().iterator().next().
				getBaseTableNode().getTabletNode().getTablet();
		if(Arrays.asList(formats).contains(tablet.getFormat())) {
			// check column
			Set<String> tableColumns = table.columns(query, false);
			Set<String> unknownCols = CollectionUtil.diffSets(tableColumns, cols);
			if(!unknownCols.isEmpty())
				errors.add("unknown columns: " + unknownCols);
		}
		// if error
		if(errors.size() > 0) throw new WCoreException(Joiner.on("|").join(errors));
	}

	/**
	 * validate query
	 */
	protected void validateQuery(RQuery query) throws WCoreException {
		// validate bday, eday
		Date bday = query.getBday();
		Date eday = query.getEday();
		if(bday.after(eday))
			throw new WCoreException("invalid query date: " + bday + "-" + eday);
		// validate cols
		RColumn[] cols = query.getCols();
		if(cols == null || cols.length < 1) {
			RColumn col = new RColumn("g_all", "string");
			col.setExType(CoreConstants.DERIVE_MODE_VIRTUAL);
			query.addColumn(col);
		}
		// validate metrics
		RMetric[] ms = query.getMs();
		if(ms == null || ms.length < 1)
			throw new WCoreException("sql error: empty query metrics!");
	}

	// validate partition
	protected void validatePartition(RTable table, RQuery query,
                                     RPartition partition, String cluster) throws WCoreException {
		// check dicts
		String eday = TimeUtil.DateToString(query.getEday());
		for (RDict dict : partition.getDicts()) {
			String dictName = dict.getDict().getDictName();
			MTableNode tNode = table.getDicts().get(dictName);
			if (!tNode.coverDays(cluster).contains(eday))
				throw new WCoreException(dictName + " unsupported calculate days: " + eday);
		}
	}
	
}
