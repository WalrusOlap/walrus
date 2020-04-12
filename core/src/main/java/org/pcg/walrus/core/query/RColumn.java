package org.pcg.walrus.core.query;

import org.pcg.walrus.core.CoreConstants;
import org.pcg.walrus.meta.pb.WFieldMessage;

import java.util.ArrayList;
import java.util.List;

/**
* Class <code>RColumn</code>
* walrus query column
*/
public class RColumn extends RField {

	private static final long serialVersionUID = -2635021468663034246L;

	private WFieldMessage.WDimension dim;
	private WFieldMessage.WDimension joinDim;
	
	public RColumn(String name){
		super(name);
	}
	
	public RColumn(String name, String type){
		super(name, type);
	}

	// column derive mode
	public String getDerivedMode() {
		if(exType != null) return exType;
		else if(dim != null) return dim.getDerivedMode();
		else return CoreConstants.DERIVE_MODE_SELECT;
	}
	
	@Override
	public RColumn copy() {
		RColumn r = new RColumn(getName(), getType());
		r.setDim(dim);
		r.setAlias(alias);
		r.setRefer(refer);
		r.setExType(exType);
		r.setJoinDim(joinDim);
		return r;
	}

	// add metric
	public static RColumn[] addColumn(RColumn[] cols, RColumn col) {
		List<RColumn> rs = new ArrayList<>();
		boolean contain = false;
		for (RColumn f : cols) {
			if(f.getName().equals(col.getName())) {
				contain = true;
				rs.add(col);
			}
			else rs.add(f);
		}
		if(!contain) rs.add(col);
		return rs.toArray(new RColumn[rs.size()]);	}

	// add metric
	public static RColumn[] removeColumn(RColumn[] cols, RColumn col){
		if(!contains(cols, col)) return cols;
		List<RColumn> rs = new ArrayList<>();
		for (RColumn f : cols)
			if(!f.getName().equals(col.getName())) rs.add(f);
		return rs.toArray(new RColumn[rs.size()]);
	}

	// convert String to columns
	public static RColumn[] toColumns(String[] cols) {
		RColumn[] columns = new RColumn[cols.length];
		int index = 0;
		for(String col: cols) columns[index++] = new RColumn(col);
		return columns;
	}

	public RColumn(WFieldMessage.WDimension dim) {
		super(dim.getName(), dim.getType());
		this.dim = dim;
	}

	@Override
	public List<String> getParamList() {
		if(dim != null && dim.getDerivedLogic() != null)
			return dim.getDerivedLogic().getParamsList();
		else return null;
	}

	public WFieldMessage.WDimension getDim() {
		return dim;
	}

	public void setDim(WFieldMessage.WDimension dim) {
		this.dim = dim;
	}

	public WFieldMessage.WDimension getJoinDim() {
		return joinDim;
	}

	public void setJoinDim(WFieldMessage.WDimension joinDim) {
		this.joinDim = joinDim;
	}
}
