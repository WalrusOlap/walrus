package org.pcg.walrus.core.query;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Joiner;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.pcg.walrus.core.CoreConstants;
import org.pcg.walrus.meta.MetaConstants;
import scala.annotation.meta.param;

public abstract class RField implements Serializable {

	private static final long serialVersionUID = -6599392191595677360L;

	protected String name;
	protected String type;

	protected String exType; // extend type: join,ext1,ext2...
	protected String alias; // alias
	protected String refer; // table refer
	protected String param;

	private Object defaultVal;

	public RField(String name) {
		this.name = name;
	}

	public RField(String name, String type) {
		this.name = name;
		this.type = type;
	}
	
	public String getName(){
		return name;
	}
	
	public String getType(){
		if(StringUtils.isEmpty(type)) type = CoreConstants.COLUMN_TYPE_INT;
		switch (type) {
		case CoreConstants.COLUMN_TYPE_DOUBLE:
			return CoreConstants.COLUMN_TYPE_DOUBLE;
		case CoreConstants.COLUMN_TYPE_STRING:
		case CoreConstants.COLUMN_TYPE_CHARARRAY:
		case CoreConstants.COLUMN_TYPE_TEXT:
			return CoreConstants.COLUMN_TYPE_STRING;
		case CoreConstants.COLUMN_TYPE_LONG:
			return CoreConstants.COLUMN_TYPE_LONG;
		case CoreConstants.COLUMN_TYPE_INT:
		default:
			return CoreConstants.COLUMN_TYPE_INT;
		}
	}

	/**
	 * spark sql data type
	 */
	public DataType getSparkType(){
		if(StringUtils.isEmpty(type)) type = CoreConstants.COLUMN_TYPE_INT;
		switch (type.toUpperCase()) {
		case CoreConstants.COLUMN_TYPE_DECIMAL:
		case CoreConstants.COLUMN_TYPE_DOUBLE_PRECISION:
		case CoreConstants.COLUMN_TYPE_DOUBLE:
			return DataTypes.DoubleType;
		case CoreConstants.COLUMN_TYPE_NUMBER:
		case CoreConstants.COLUMN_TYPE_BIGINT:
		case CoreConstants.COLUMN_TYPE_LONG:
			return DataTypes.LongType;
		case CoreConstants.COLUMN_TYPE_BYTEA:
		case CoreConstants.COLUMN_TYPE_STRING:
		case CoreConstants.COLUMN_TYPE_CHARARRAY:
		case CoreConstants.COLUMN_TYPE_CHAR:
		case CoreConstants.COLUMN_TYPE_TEXT:
		case CoreConstants.COLUMN_TYPE_NCHAR:
		case CoreConstants.COLUMN_TYPE_VARCHAR:
			return DataTypes.StringType;
		case CoreConstants.COLUMN_TYPE_INT:
		case CoreConstants.COLUMN_TYPE_TINYINT:
		case CoreConstants.COLUMN_TYPE_SMALLINT:
		default:
			return DataTypes.IntegerType;
		}
	}

	/**
	 * set default value
	 */
	public void setDefaultVal(Object val) {
		this.defaultVal = val;
	}

	/**
	 * default value
	 */
	public Object getDefaultVal() {
		if(defaultVal != null) return defaultVal;
		switch (type.toUpperCase()) {
		case CoreConstants.COLUMN_TYPE_STRING:
		case CoreConstants.COLUMN_TYPE_CHARARRAY:
		case CoreConstants.COLUMN_TYPE_CHAR:
		case CoreConstants.COLUMN_TYPE_TEXT:
		case CoreConstants.COLUMN_TYPE_NCHAR:
		case CoreConstants.COLUMN_TYPE_VARCHAR:
			return "''";
		case CoreConstants.COLUMN_TYPE_DATE:
			return MetaConstants.DEFAULT_BDAY;
		case CoreConstants.COLUMN_TYPE_TIMESTAMP:
		case CoreConstants.COLUMN_TYPE_DOUBLE:
		case CoreConstants.COLUMN_TYPE_INT:
		case CoreConstants.COLUMN_TYPE_DECIMAL:
		case CoreConstants.COLUMN_TYPE_LONG:
		case CoreConstants.COLUMN_TYPE_NUMBER:
		case CoreConstants.COLUMN_TYPE_DOUBLE_PRECISION:
		case CoreConstants.COLUMN_TYPE_TINYINT:
		case CoreConstants.COLUMN_TYPE_SMALLINT:
		case CoreConstants.COLUMN_TYPE_BIGINT:
		case CoreConstants.COLUMN_TYPE_BYTEA:
		default:
			return 0;
		}
	}

	public abstract String getDerivedMode();
	public abstract List<String> getParamList();
	public abstract RField copy();

	protected static boolean contains(RField[] fields, RField field) {
		boolean contains = false;
		for(RField f: fields) {
			if(f.getName().equals(field.getName())) {
				contains = true;
				break;
			}
		}
		return contains;
	}

	// convert field to string
	public static String[] toFieldArray(RField[] fields) {
		String[] array = new String[fields.length];
		int index = 0;
		for(RField field: fields) array[index++] = field.getName();
		return array;
	}

	// field derive logic
	public void setExType(String exType) {
		this.exType = exType;
	}

	public String getAlias() {
		if(alias != null && alias.length() > 0) return alias;
		else return name; // default name as alisa
	}

	public void setParam(String param) {
		this.param = param;
	}

	public String getParam() {
		if(param != null && param.length() > 0) return param;
		else if (getParamList() != null) return Joiner.on(",").join(getParamList());
		else return null;
	}

	public void setAlias(String alias) {
		this.alias = alias;
	}

	public String getRefer() {
		return refer;
	}

	public void setRefer(String refer) {
		this.refer = refer;
	}

	@Override
	public boolean equals(Object obj) {
        if (obj instanceof RField) {   
        	RField c = (RField) obj;   
            return name.equalsIgnoreCase(c.getName());   
        }
        return super.equals(obj);
	}

	@Override
	public int hashCode() {
		return name.hashCode();
	}

	@Override
	public String toString() {
		return getName() + "[" + getDerivedMode() + "," + getParam() + "]";
	}

}
