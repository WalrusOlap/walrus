package org.pcg.walrus.core.query;

import java.io.Serializable;
import java.text.ParseException;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;
import org.pcg.walrus.core.CoreConstants;
import org.pcg.walrus.common.exception.WCoreException;
import org.pcg.walrus.meta.pb.WFieldMessage;
import org.pcg.walrus.meta.pb.WTableMessage;
import org.pcg.walrus.meta.tree.MPartitionNode;
import org.pcg.walrus.meta.tree.MTableNode;
import org.pcg.walrus.common.util.TimeUtil;

/**
* Class <code>RTable</code> <br>
* walrus virtual table, contains fact table and join dicts
*/
public class RTable implements Comparable<RTable>, Serializable {

	private static final long serialVersionUID = 2056045366665087770L;

	private long priority = CoreConstants.TABLE_PRIORITY_DEFAULT;

	private MTableNode tableNode;
	private Set<WFieldMessage.WDict> joins;
	private Map<String, MTableNode> dicts;

	public RTable(MTableNode tableNode, Set<WFieldMessage.WDict> joins,
			Map<String, MTableNode> dicts) {
		this.tableNode = tableNode;
		this.joins = joins;
		this.dicts = dicts;
	}

	/**
	 * return all columns in <CODE>tableNode</CODE> and can be joined by <CODE>joins</CODE>
	 */
	public Set<String> columns(RQuery query, boolean joinDict) throws WCoreException {
		Date bday = query.getBday();
		Date eday = query.getEday();
		String level = query.getLevel();
		Set<String> columns = Sets.newHashSet();
		Set<String> cols = tableNode.columns(bday, eday, level, true);
		columns.addAll(cols);
		// add join columns
		if(joinDict) for(RDict d: columnDicts(query, bday, eday)) columns.addAll(d.getFields());
		return columns;
	}

	/**
	 * return <CODE>RJoin</CODE> need to join for this query metric</br>
	 * ! JOIN LOGIC is set in metric
	 */
	public Set<RDict> metricDicts(RMetric[] metrics, Date bday, Date eday) throws WCoreException {
		Set<RDict> joinDicts = Sets.newHashSet();
		if(dicts == null) return joinDicts;
		for(RMetric metric: metrics) {
			if(CoreConstants.DERIVE_MODE_JOIN.equals(metric.getDerivedMode())) {
				WFieldMessage.WDict wd = metric.getMetric().getDict();
				String dictName = wd.getDictName();
				if(!dicts.containsKey(dictName))
					throw new WCoreException("No dict found for " + metric.getName() + "(" + dictName + ")");
				MTableNode dict = dicts.get(dictName);
				MPartitionNode table = joinDict(dict, bday, eday);
				String dTable = table.getBaseTableNode().getTabletNode().getName();
				RDict join = new RDict(dTable, wd, null);
				joinDicts.add(join);
			}
		}
		return joinDicts;
	}

	/**
	 * return <CODE>RJoin</CODE> need to join for this query column</br>
	 * ! JOIN LOGIC is configued in dicts
	 * @throws WCoreException
	 */
	public Set<RDict> columnDicts(RQuery query, Date bday, Date eday) throws WCoreException {
		String level = query.getLevel();
		Set<RDict> joinDicts = Sets.newHashSet();
		if(dicts == null || joins == null) return joinDicts;
		Set<String> columns = tableNode.dims(bday, eday, level, true).keySet();
		if(columns == null || columns.isEmpty()) return joinDicts;
		int dictPriority = 1;
		for (WFieldMessage.WDict d : joins)
			recursiveDict(d, null, joinDicts, columns, query.realColumns(), bday, eday, level, dictPriority);
		return joinDicts;
	}

	// recursive dict, find all necessary dict
	private void recursiveDict(WFieldMessage.WDict d, RDict parentDict, Set<RDict> joinDicts,
		   Set<String> columns, Set<String> queryCols, Date bday, Date eday,
		   String level, int dictPriority) throws WCoreException {
		// if dict not exists, return
		if (!dicts.containsKey(d.getDictName())) return;
		// if fact join keys not in parent, return
		boolean hasJoinParentKey = true;
		Set<String> dCols = null;
		if(parentDict != null) dCols = parentDict.getFields();
		else dCols = columns;
		for(String k: d.getFactKeysList())
			hasJoinParentKey = hasJoinParentKey & dCols.contains(k);
		if(!hasJoinParentKey) return;

		MTableNode dict = dicts.get(d.getDictName());
		Set<String> cs = dict.columns(bday, eday, level, true);
		if(cs == null || cs.isEmpty()) return;
		boolean need = false;
		MPartitionNode table = joinDict(dict, bday, eday);
		String dTable = table.getBaseTableNode().getTabletNode().getName();
		RDict join = new RDict(dTable, d, parentDict);
		join.setPriority(dictPriority);
		Set<String> qCols = new HashSet<>();
		for (String c : queryCols) {
			boolean found = false;
			// if column not in table & column in dict, add dict
			for(WFieldMessage.WDimension dim: table.getPartition().getDimensionsList()) {
				// add all column for child join
				join.addField(dim.getName());
				if(!columns.contains(c) && cs.contains(c) && dim.getName().equals(c)) {
					need = true;
					found = true;
					join.addJoinColumn(new RColumn(dim));
				}
			}
			// if column is found, no need to join child dict
			if(!found) qCols.add(c);
		}
		// if need join, add dict and all parent dict
		if(need) {
			RDict parent = join.parent();
			int p = dictPriority - 1;
			while(parent != null) {
				if(!joinDicts.contains(parent)) {
					parent.setPriority(p);
					joinDicts.add(parent);
				}
				parent = parent.parent();
				p = p - 1;
			}
			joinDicts.add(join);
		}
		// recursive child dicts
		for(WFieldMessage.WDict childD: d.getChildDictList())
			recursiveDict(childD, join, joinDicts, columns, qCols, bday, eday, level, dictPriority + 1);
	}

	/**
	 * if column is joined by a extend key column, add extend key
	 */
	public RColumn[] addColumnJoinLogic(Set<RDict> dicts, RColumn[] columns, List<WFieldMessage.WDimension> dims) {
		Set<RColumn> cols = new HashSet<RColumn>();
		for (RDict dict : dicts) {
			for (String lk : dict.getDict().getFactKeysList()) {
				for (WFieldMessage.WDimension dim : dims) {
					if (lk.equals(dim.getName())
							&& CoreConstants.DERIVE_MODE_EXTEND.equals(dim.getDerivedMode())) {
						RColumn col = new RColumn(dim);
						col.setExType(CoreConstants.DERIVE_MODE_EXT2);
						cols.add(col);
					}
				}
			}
		}
		RColumn[] retCols = new RColumn[columns.length + cols.size()];
		int index = 0;
		for(RColumn col: columns) retCols[index++] = col;
		for(RColumn col: cols) retCols[index++] = col;
		return retCols;
	}

	/**
	 * if metric is joined by a dict,e.g. income, add join logic
	 */
	public RMetric[] addMetricJoinLogic(RMetric[] metrics, Date bday, Date eday) throws WCoreException {
		if(dicts == null) return metrics;
		RMetric[] ms = new RMetric[metrics.length];
		int index = 0;
		for(RMetric metric: metrics) {
			if(CoreConstants.DERIVE_MODE_JOIN.equals(metric.getDerivedMode())) {
				String mName = metric.getName();
				WFieldMessage.WDict wd = metric.getMetric().getDict();
				String dictName = wd.getDictName();
				if(!dicts.containsKey(dictName))
					throw new WCoreException("No dict found for " + metric.getName());
				MTableNode dict = dicts.get(dictName);
				WTableMessage.WPartitionTable t = joinDict(dict, bday, eday).getPartition();
				if(t == null) throw new WCoreException("Wrong partition mode for " + dictName);
				for(WFieldMessage.WMetric m: t.getMetricsList()) {
					if(mName.equals(m.getName())) {
						// join metric derive mode = extend
						if(CoreConstants.DERIVE_MODE_EXTEND.equals(m.getDerivedMode())) {
							metric.setJoinMetric(m);
							// ext type
							metric.setExType(CoreConstants.DERIVE_MODE_EXT1);
						}
					}
				}
			}
			ms[index++] = metric;
		}
		return ms;
	}

	// find a join dict with right partition
	private MPartitionNode joinDict(MTableNode dict, Date bday, Date eday) throws WCoreException {
		for(String partition: dict.getPartitions().keySet()) {
			MPartitionNode partitionNode = dict.getPartitions().get(partition);
			try {
				int start = TimeUtil.dateToInt(TimeUtil.stringToDate(partitionNode.getBaseTableNode().getTable().getStartTime()));
				int end = TimeUtil.dateToInt(TimeUtil.stringToDate(partitionNode.getBaseTableNode().getTable().getEndTime()));
				if (start <= TimeUtil.dateToInt(eday) && end >= TimeUtil.dateToInt(bday))
					return partitionNode;
			}
			catch(ParseException e){
				throw new WCoreException(partition + " wrong time format: " + e.getMessage());
			}
		}
		throw new WCoreException("no partition found for dict: " + dict.getName());
	}

	public long getPriority() {
		return priority;
	}

	public void setPriority(long priority) {
		this.priority = priority;
	}

	public MTableNode getTableNode() {
		return tableNode;
	}

	public Set<WFieldMessage.WDict> getJoins() {
		return joins;
	}

	public Map<String, MTableNode> getDicts() {
		return dicts;
	}

	@Override
	public int compareTo(RTable unit) {
		return (int)(unit.priority - priority) ;
	}
	
	@Override
	public int hashCode() {
		return tableNode.getName().hashCode();
	}

	@Override
	public boolean equals(Object obj) {
        if (obj instanceof RTable) {   
        	RTable c = (RTable) obj;
            return tableNode.getName().equalsIgnoreCase(c.tableNode.getName());   
        }
        return super.equals(obj);
	}
	
	@Override
	public String toString() {
		String joinStr = "";
		for(WFieldMessage.WDict d: joins) joinStr +=  d.getDictName() + "\t";
		return tableNode.getName() + "[" + joinStr + "]";
	}
}