package org.pcg.walrus.common.io;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.google.common.collect.Maps;

/**
 * HdfsJavaReader load dataframe for spark sql context
 */
public class HdfsJavaReader implements Serializable {

	private static final long serialVersionUID = -2981231566724788165L;

	private static long PARTITION_MIN_SIZE = 100 * 1024 * 1024; // min data size each partition
	private static long PARTITION_MAX_SIZE = 400 * 1024 * 1024; // max data size each partition

	/**
	 * parquet loader
	 */
	public static Dataset<Row> loadParquet(SQLContext context, String path) {
		return context.read().parquet(path);
	}

	/**
	 * @param schema: col1:col_type,col2_col_type...
	 * @param recNum: record number
	 * @param fileNum: file number
	 */
	public static Dataset<Row> loadCsv(JavaSparkContext sc, SQLContext context,
			String path, String schema, final String delim, long recNum, long fileNum) throws Exception {
		final Map<Integer, Col> map = parseSchema(schema);
		JavaRDD<String> data = sc.textFile(path);
		// load data
		JavaRDD<Row> rowRDD = data.map(new Function<String, Row>() {
			private static final long serialVersionUID = 1L;
			public Row call(String record) throws Exception {
				Object[] vals = new Object[map.size()];
				String[] fields = record.split(delim, -1);
				for(int i=0; i < map.size(); i++) {
					Col col = map.get(i);
					try {
						vals[i] = col.parseVal(fields[i]);
					} catch (Exception e) {
						vals[i] = col.getDefaultVal();
					}
				}
				return RowFactory.create(vals);
			}
		});

		// Apply the schema to the RDD.
		List<StructField> fields = new ArrayList<StructField>();
		for(int i=0; i<map.size(); i++) {
			Col col = map.get(i);
			fields.add(DataTypes.createStructField(col.name, col.getType(), true));
		}
		// create df
		StructType fieldSchema = DataTypes.createStructType(fields);
		Dataset<Row> df = context.createDataFrame(rowRDD, fieldSchema);
		// if too many files, coalesce
		if(recNum>0 && fileNum>0 && recNum / PARTITION_MIN_SIZE < (fileNum * 2)) {
			int numPartitions = (int) (recNum / PARTITION_MIN_SIZE);
			if(numPartitions > 0) df = df.coalesce(numPartitions);
		}
		// if too less files, repartition
		if(recNum>0 && fileNum>0 && recNum / PARTITION_MAX_SIZE > fileNum) {
			int numPartitions = (int) (recNum / PARTITION_MAX_SIZE / 2);
			if(numPartitions > 0) df = df.repartition(numPartitions);
		}
		return df;
	}
	
	// parse pig schema -> index: (column, type)
	private static Map<Integer, Col> parseSchema(String schema) throws Exception {
		Map<Integer, Col> map = Maps.newHashMap();
		int index = 0;
		for (String field : schema.split("\\s*,\\s*")) {
			String[] fs = field.split("\\s*:\\s*");
			if (fs.length != 2)
				throw new Exception("invalid schema: " + field);
			Col col = new Col(fs[0].trim(), fs[1].trim());
			map.put(index++, col);
		}
		return map;
	}

	/**
	 * column case class
	 */
	private static class Col implements Serializable {

		private static final long serialVersionUID = 4810304483160256461L;
		public String name;
		public String type;
		
		//data type
		private static final String COLUMN_TYPE_INT = "int";
		private static final String COLUMN_TYPE_TEXT = "text";
		private static final String COLUMN_TYPE_STRING = "string";
		private static final String COLUMN_TYPE_CHARARRAY = "chararray";
		private static final String COLUMN_TYPE_LONG = "long";
		private static final String COLUMN_TYPE_DOUBLE = "double";

		public Col(String name, String type) {
			this.name = name;
			this.type = type;
		}

		public DataType getType() {
			switch (type) {
			case COLUMN_TYPE_DOUBLE:
				return DataTypes.DoubleType;
			case COLUMN_TYPE_TEXT:
			case COLUMN_TYPE_STRING:
			case COLUMN_TYPE_CHARARRAY:
				return DataTypes.StringType;
			case COLUMN_TYPE_LONG:
				return DataTypes.LongType;
			case COLUMN_TYPE_INT:
			default:
				return DataTypes.IntegerType;
			}
		}
		
		public Object getDefaultVal() {
			switch (type) {
			case COLUMN_TYPE_DOUBLE:
				return 0d;
			case COLUMN_TYPE_TEXT:
			case COLUMN_TYPE_STRING:
			case COLUMN_TYPE_CHARARRAY:
				return "";
			case COLUMN_TYPE_LONG:
				return 0l;
			case COLUMN_TYPE_INT:
			default:
				return 0;
			}
		}

		public Object parseVal(String val){
			Object value = null;
			switch (type) {
			case COLUMN_TYPE_INT:
				try {
					value = Integer.parseInt(val);
				} catch (Exception e) {
					value = 0;
				}
				break;
			case COLUMN_TYPE_DOUBLE:
				try {
					value = Double.parseDouble(val);
				} catch (Exception e) {
					value = 0.0;
				}
				break;
			case COLUMN_TYPE_TEXT:
			case COLUMN_TYPE_STRING:
			case COLUMN_TYPE_CHARARRAY:
				try {
					value = val.trim();
				} catch (Exception e) {
					value = "";
				}
				break;
			case COLUMN_TYPE_LONG:
				try {
					value = Long.parseLong(val);
				} catch (Exception e) {
					value = 0L;
				}
				break;
			default:
				value = 0;
				break;
			}
			return value;
		}
		
		@Override
		public String toString() {
			return name + "(" + type + ")";
		}
	}
}
