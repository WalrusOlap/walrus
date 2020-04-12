package org.pcg.walrus.meta;

import java.util.*;

import com.google.common.collect.Sets;
import org.pcg.walrus.meta.tree.MTableNode;
import org.pcg.walrus.meta.tree.MViewTableNode;
import org.pcg.walrus.meta.pb.WFieldMessage;
import org.pcg.walrus.meta.pb.WViewMessage;

/**
 * MetaUtils </br>
 * 	static functions to visit meta tree
 */
public class MetaUtils {

	// return all columns in this view
	public static Set<String> getViewColumns(WViewMessage.WView view,
											 Map<String, MTableNode> tables, Map<String, MTableNode> dicts,
											 Date bday, Date eday, String level) {
		Set<String> columns = Sets.newHashSet();
		columns.addAll(getViewDs(view, tables, dicts, bday, eday, level));
		columns.addAll(getViewMs(view, tables, dicts, bday, eday, level));
		return columns;
	}

	// return all dimensions in this view
	public static Set<String> getViewDs(WViewMessage.WView view,
			Map<String, MTableNode> tables, Map<String, MTableNode> dicts, Date bday, Date eday, String level) {
		Set<String> ms = new HashSet();
		for(String m: getViewDims(view, tables, dicts, bday, eday, level).keySet()) ms.add(m);
		return ms;
	}

	// return all metrics in this view
	public static Set<String> getViewMs(WViewMessage.WView view,
			Map<String, MTableNode> tables, Map<String, MTableNode> dicts,
			Date bday, Date eday, String level) {
		Set<String> ms = new HashSet();
		for(String m: getViewMetrics(view, tables, dicts, bday, eday, level).keySet()) ms.add(m);
		return ms;
	}

	// return all metrics in this view
	public static Map<String, WFieldMessage.WMetric> getViewMetrics(WViewMessage.WView view,
																	Map<String, MTableNode> tables, Map<String, MTableNode> dicts,
																	Date bday, Date eday, String level) {
		Map<String, WFieldMessage.WMetric> metrics = new HashMap<>();
		scanView(tables, dicts, view, MetaConstants.META_FIELD_METRIC, metrics, null, bday, eday, level, true);
		return metrics;
	}

	// return all dims in this view
	public static Map<String, WFieldMessage.WDimension> getViewDims(WViewMessage.WView view,
			Map<String, MTableNode> tables, Map<String, MTableNode> dicts,
			Date bday, Date eday, String level) {
		Map<String, WFieldMessage.WDimension> columns = new HashMap<>();
		scanView(tables, dicts, view, MetaConstants.META_FIELD_DIM, null, columns, bday, eday, level, true);
		return columns;
	}

	// return all dims in this view
	public static Set<String> getViewCoverDays(WViewMessage.WView view, Map<String, MTableNode> tables, String level) {
		Set<String> days = Sets.newHashSet();
		scanViewDays(view, tables, days, level);
		return days;
	}
	
	// get view cover days
	private static void scanViewDays(WViewMessage.WView view, Map<String, MTableNode> tables, Set<String> days, String level) {
		if(view.getUnionsList().size() == 0) {
			if(tables.containsKey(view.getName()))
				days.addAll(tables.get(view.getName()).coverDays(level));
			for (WViewMessage.WCube r : view.getCubesList()) scanRollupDays(r, tables, days, level);
		} else for(WViewMessage.WView v: view.getUnionsList()) scanViewDays(v, tables, days, level);
	}

	// recursive rollups
	private static void scanRollupDays(WViewMessage.WCube cube,
			Map<String, MTableNode> tables, Set<String> days, String level) {
		if(tables.containsKey(cube.getName()))
			days.addAll(tables.get(cube.getName()).coverDays(level));
		for (WViewMessage.WCube r : cube.getCubesList())
			scanRollupDays(r, tables, days, level);
	}

	// recursive view
	private static void scanView(Map<String, MTableNode> tables,
			Map<String, MTableNode> dicts, WViewMessage.WView view, String field,
			Map<String, WFieldMessage.WMetric> metrics, Map<String, WFieldMessage.WDimension> columns,
			Date bday, Date eday, String level, boolean isIntersection) {
		if (view.getUnionsList().size() == 0) {
			// base
			MTableNode table = tables.get(view.getName());
			if(table != null) {
				switch (field) {
				case MetaConstants.META_FIELD_METRIC:
					metrics.putAll(table.metrics(bday, eday, level, isIntersection));
					break;
				case MetaConstants.META_FIELD_DIM:
					columns.putAll(table.dims(bday, eday, level, isIntersection));
					break;
				default:
					break;
				}
			}
			// rollup
			for(WViewMessage.WCube r : view.getCubesList())
				scanCube(tables, r, field, bday, eday, level, isIntersection, metrics, columns);
		} else {
			if (MetaConstants.META_FIELD_DIM.equals(field)) {
				for(WFieldMessage.WDict d: view.getJoinsList())
					scanDict(dicts, d, columns, bday, eday, level, isIntersection);
			}
			for(WViewMessage.WView v : view.getUnionsList())
				scanView(tables, dicts, v, field, metrics, columns, bday, eday, level, isIntersection);
		}
	}

	// recursive dict
	private static void scanDict(Map<String, MTableNode> dicts,
								 WFieldMessage.WDict d, Map<String, WFieldMessage.WDimension> columns,
			 Date bday, Date eday, String level, boolean isIntersection) {
		MTableNode dict = dicts.get(d.getDictName());
		if(dict == null) return;
		// add dict columns
		columns.putAll(dict.dims(bday, eday, level, isIntersection));
		// add child dict columns
		for(WFieldMessage.WDict child: d.getChildDictList()) scanDict(dicts, child, columns, bday, eday, level, isIntersection);
	}

	// recursive cube
	private static void scanCube(Map<String, MTableNode> tables, WViewMessage.WCube cube,
			String field, Date bday, Date eday, String level, boolean isIntersection,
			Map<String, WFieldMessage.WMetric> metrics, Map<String, WFieldMessage.WDimension> cols) {
		MTableNode table = tables.get(cube.getName());
		if(table == null) return;
		switch (field) {
		case MetaConstants.META_FIELD_METRIC:
			Map<String, WFieldMessage.WMetric> ms = table.metrics(bday, eday, level, isIntersection);
			if(ms != null) metrics.putAll(ms);
			break;
		case MetaConstants.META_FIELD_DIM:
			Map<String, WFieldMessage.WDimension> dims = table.dims(bday, eday, level, isIntersection);
			if(dims != null) cols.putAll(dims);
			break;
		default:
			break;
		}
		for(WViewMessage.WCube r : cube.getCubesList())
			scanCube(tables, r, field, bday, eday, level, isIntersection, metrics, cols);
	}

	// get all tables in this view
	public static Set<MViewTableNode> getViewTables(WViewMessage.WView view) {
		Set<MViewTableNode> tables = Sets.newHashSet();
		scanView(view, tables);
		return tables;
	}

	// recursive view cubes
	private static void scanView(WViewMessage.WView view, Set<MViewTableNode> tables) {
		if(view.getUnionsList().size() == 0) {
			MViewTableNode n = new MViewTableNode(view.getName(), MetaConstants.TABLE_PRIORITY_DEFAULT);
			tables.add(n);
			for (WViewMessage.WCube r : view.getCubesList()) addCube(r, tables);
		} else for(WViewMessage.WView v: view.getUnionsList()) scanView(v, tables);
	}

	// recursive cubes
	private static void addCube(WViewMessage.WCube cube, Set<MViewTableNode> tables) {
		MViewTableNode n = new MViewTableNode(cube.getName(), cube.getPriority());
		tables.add(n);
		for (WViewMessage.WCube r : cube.getCubesList()) addCube(r, tables);
	}
}
