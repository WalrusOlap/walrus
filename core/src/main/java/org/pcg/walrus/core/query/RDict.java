package org.pcg.walrus.core.query;

import com.google.common.collect.Lists;
import org.pcg.walrus.meta.pb.WFieldMessage;

import java.io.Serializable;
import java.util.*;

/**
 * RJoin</br> join information
 */
public class RDict implements Serializable, Comparable<RDict> {

	private static final long serialVersionUID = 3965878423422573636L;

	private int priority = 1; // dict join priority
	private RDict parentDict; // is null when join fact table

	private String table;
	private WFieldMessage.WDict dict;

	private List<RColumn> joinColumns; // join column to query
	private Set<String> fields; // dict all fields, for child join

	public RDict(String table, WFieldMessage.WDict dict, RDict parentDict) {
		this.table = table;
		this.dict = dict;
		this.parentDict = parentDict;
		joinColumns = new ArrayList<RColumn>();
		fields = new HashSet<>();
	}

	/**
	 * deep copy
	 */
	public RDict copy() {
		RDict r = new RDict(table, dict, parentDict);
		r.joinColumns = this.joinColumns;
		r.fields = this.fields;
		r.priority = this.priority;
		return r;
	}

	public List<RColumn> getJoinColumns() {
		return joinColumns;
	}

	public Set<String> getFields() {
		return fields;
	}

	public void addField(String column) {
		fields.add(column);
	}

	public void addJoinColumn(RColumn column) {
		this.joinColumns.add(column);
	}

	public String getTable() {
		return table;
	}

	public String getDictName() {
		return dict.getDictName();
	}

	public String getJoinType() {
		return dict.getJoinType();
	}

	public String[] getFactKeysList() {
		if(dict.getFactKeysList() != null )
		return dict.getFactKeysList().toArray(new String[dict.getFactKeysList().size()]);
		else return new String[]{};
	}

	public String[] getDictKeysList() {
		if(dict.getDictKeysList() != null )
			return dict.getDictKeysList().toArray(new String[dict.getDictKeysList().size()]);
		else return new String[]{};
	}

	private void updateJoinKey(String[] keys, String originKey, String updateKey) {
		for(int i = 0; i < keys.length; i++) {
			if(keys[i].equalsIgnoreCase(originKey)){
				keys[i] = updateKey;
				break;
			}
		}
	}

	public void setTable(String table) {
		this.table = table;
	}

	public void setPriority(int priority) {
		this.priority = priority;
	}

	// get
	public RDict parent() {
		return parentDict;
	}

	@Override
	public int hashCode() {
		return table.hashCode();
	}

	@Override
	public int compareTo(RDict dict) {
		return priority - dict.priority;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof RDict) {
			RDict c = (RDict) obj;
			return table.equalsIgnoreCase(c.table);
		}
		return super.equals(obj);
	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer("{");
		if(parentDict != null) sb.append(String.format("%s.[%d]", parentDict.table, priority));
		sb.append(String.format("%s, dictKeys: %s, factKeys: %s}", table,
				dict.getDictKeysList(), dict.getFactKeysList()));
		return sb.toString();
	}

	public WFieldMessage.WDict getDict() {
		return dict;
	}

	public void setDict(WFieldMessage.WDict dict) {
		this.dict = dict;
	}

}
