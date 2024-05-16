package com.spinsys.mdaca.storage.explorer.bigdata;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.sql.rowset.RowSetMetaDataImpl;

import org.apache.hadoop.hive.metastore.api.FieldSchema;

import com.spinsys.mdaca.storage.explorer.model.exception.ExplorerException;
import com.univocity.parsers.common.AbstractParser;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

import tech.tablesaw.api.ColumnType;
//import tech.tablesaw.api.Table;
//import tech.tablesaw.columns.Column;
import tech.tablesaw.io.csv.CsvReadOptions;
import tech.tablesaw.io.csv.CsvReader;

public class CSVFileProcessor {

	private static final Logger logger =
            Logger.getLogger("com.spinsys.mdaca.storage.explorer.bigdata.CSVFileProcessor");


	public static void getCSVData(String filePath) throws IOException {
	}

	public ResultSetMetaData getSchema(String filePath) throws IOException, SQLException {
		RowSetMetaDataImpl schema = new RowSetMetaDataImpl();
		
		// see https://jtablesaw.github.io/tablesaw/userguide/importing_data.html
		// for available options
		CsvReadOptions options = CsvReadOptions.builder(filePath).build();
		CsvReader csvReader = new CsvReader();
//		String types = csvReader.printColumnTypes(options);
		/* returns something like:
				ColumnType[] columnTypes = {
				INTEGER,    // 0     idstate       
				STRING,     // 1     labelstate    
				STRING,     // 2     fullstatename 
				}
		 */
//		String types = CsvReader.printColumnTypes(filePath, true, ','));
		
		AbstractParser<?> parser = createCsvParser(options);
		Reader fileReader = new FileReader(filePath);
		int linesToSkip = 1; // header line(s)
		ColumnType[] inferredColumnTypes =
				csvReader.getColumnTypes(fileReader, options, linesToSkip, parser);
		schema.setColumnCount(inferredColumnTypes.length);
		
		CsvReader csvReader2 = new CsvReader();
		AbstractParser<?> parser2 = createCsvParser(options);
		Reader fileReader2 = new FileReader(filePath);
		parser2.beginParsing(fileReader2);
		String[] columnNames =
				csvReader2.getColumnNames(options, inferredColumnTypes, parser2);
		
		for (int i = 0; i < inferredColumnTypes.length; i++) {
			// Columns are numbered starting with 1, not 0
			int colNo = i + 1;
			schema.setColumnTypeName(colNo, inferredColumnTypes[i].name());
			String colName = (i < columnNames.length)
					? columnNames[i] 	// normal case
					: ("col" + colNo); // surplus data, generate a column name
			schema.setColumnName(colNo, colName);
		}

//		Table table = Table.read().usingOptions(CsvReadOptions
//			    .builder(filePath)
//			    .header(true)  // TODO check whether this should be parameter
//			    .missingValueIndicator("-"));
		return schema;
	}

	/*
Loading a CSV from S3:
ColumnTypes[] types = {SHORT_INT, FLOAT, SHORT_INT};
S3Object object = 
    s3Client.getObject(new GetObjectRequest(bucketName, key));

InputStream stream = object.getObjectContent();
Table t = Table.csv(CsvReadOptions.builder(stream)
    .tableName("bush")
    .columnTypes(types)));
	 */
	
	
	private static CsvParser createCsvParser(CsvReadOptions options) {
		CsvParserSettings settings = new CsvParserSettings();
		settings.setLineSeparatorDetectionEnabled(options.lineSeparatorDetectionEnabled());
//		    settings.setFormat(csvFormat(options));
		settings.setMaxCharsPerColumn(options.maxCharsPerColumn());
		if (options.maxNumberOfColumns() != null) {
			settings.setMaxColumns(options.maxNumberOfColumns());
		}
		return new CsvParser(settings);
	}

	/**
	 * Convert the types produced in the schema to some recognized
	 * by Hive/thrift
	 * @see https://github.com/apache/hive/blob/master/serde/src/gen/thrift/gen-javabean/org/apache/hadoop/hive/serde/serdeConstants.java,
	 * 		https://github.com/apache/hive/blob/0e05fd727863bef46822b5ca9f48b8a6891b219a/standalone-metastore/metastore-common/src/main/java/org/apache/hadoop/hive/metastore/ColumnType.java#L38,
	 * 		https://cwiki.apache.org/confluence/display/hive/languagemanual+types,
	 * 		https://github.com/jtablesaw/tablesaw/blob/master/core/src/main/java/tech/tablesaw/api/ColumnType.java,
	 * 		https://www.javadoc.io/doc/org.apache.parquet/parquet-column/1.8.1/org/apache/parquet/schema/PrimitiveType.PrimitiveTypeName.html
	 * @param columnTypeName
	 * @return 
	 */
	public static String translateToSerdeColumnType(String columnTypeName) {
		// TODO clean this up.  This method was once a universal translator.
		// Now, it should only translate types applicable to our CSV 
		// processing package (tablesaw).
		String serdeType = org.apache.hadoop.hive.metastore.ColumnType.STRING_TYPE_NAME;
		if (columnTypeName != null) {
			serdeType = columnTypeName.toLowerCase();

			// TODO check the various date, time conversions,
			// particularly relative to the local time zone.
			switch (serdeType) {
			case "array":
				serdeType = org.apache.hadoop.hive.metastore.ColumnType.LIST_TYPE_NAME;
				break;
			case "binary":
				serdeType = org.apache.hadoop.hive.metastore.ColumnType.BINARY_TYPE_NAME;
				break;
			case "bigint":
				serdeType = org.apache.hadoop.hive.metastore.ColumnType.BIGINT_TYPE_NAME;
				break;
			case "boolean":
				serdeType = org.apache.hadoop.hive.metastore.ColumnType.BOOLEAN_TYPE_NAME;
				break;
			case "char":
				serdeType = org.apache.hadoop.hive.metastore.ColumnType.CHAR_TYPE_NAME;
				break;
			case "date":
				serdeType = org.apache.hadoop.hive.metastore.ColumnType.DATE_TYPE_NAME;
				break;
			case "datetime":
				serdeType = org.apache.hadoop.hive.metastore.ColumnType.DATETIME_TYPE_NAME;
				break;
			case "decimal":
				serdeType = org.apache.hadoop.hive.metastore.ColumnType.DECIMAL_TYPE_NAME;
				break;
			case "double":
				serdeType = org.apache.hadoop.hive.metastore.ColumnType.DOUBLE_TYPE_NAME;
				break;
			case "fixed_len_byte_array":
				serdeType = org.apache.hadoop.hive.metastore.ColumnType.LIST_TYPE_NAME;
				break;
			case "float":
				serdeType = org.apache.hadoop.hive.metastore.ColumnType.FLOAT_TYPE_NAME;
				break;
			case "int":
			case "integer":
			case "int32":	//parquet/schema/PrimitiveType
				serdeType = org.apache.hadoop.hive.metastore.ColumnType.INT_TYPE_NAME;
				break;
			case "int64":	//parquet/schema/PrimitiveType
				serdeType = org.apache.hadoop.hive.metastore.ColumnType.BIGINT_TYPE_NAME;
				break;
			case "int96":	//parquet/schema/PrimitiveType
				serdeType = org.apache.hadoop.hive.metastore.ColumnType.TIMESTAMP_TYPE_NAME;
				break;
			case "interval_day_time":
				serdeType = org.apache.hadoop.hive.metastore.ColumnType.INTERVAL_DAY_TIME_TYPE_NAME;
				break;
			case "interval_year_month":
				serdeType = org.apache.hadoop.hive.metastore.ColumnType.INTERVAL_YEAR_MONTH_TYPE_NAME;
				break;
			case "local_date":
				serdeType = org.apache.hadoop.hive.metastore.ColumnType.DATE_TYPE_NAME;
				break;
			case "local_date_time":	// tablesaw
				serdeType = org.apache.hadoop.hive.metastore.ColumnType.DATETIME_TYPE_NAME;
				break;
			case "local_time":	// tablesaw
				serdeType = org.apache.hadoop.hive.metastore.ColumnType.TIMESTAMPTZ_TYPE_NAME;
				break;
			case "long":
				serdeType = org.apache.hadoop.hive.metastore.ColumnType.BIGINT_TYPE_NAME;
				break;
			case "map":
				serdeType = org.apache.hadoop.hive.metastore.ColumnType.MAP_TYPE_NAME;
				break;
			case "smallint":
				serdeType = org.apache.hadoop.hive.metastore.ColumnType.SMALLINT_TYPE_NAME;
				break;
			case "string":
				serdeType = org.apache.hadoop.hive.metastore.ColumnType.STRING_TYPE_NAME;
				break;
			case "struct":
				serdeType = org.apache.hadoop.hive.metastore.ColumnType.STRUCT_TYPE_NAME;
				break;
			case "text":
				serdeType = org.apache.hadoop.hive.metastore.ColumnType.STRING_TYPE_NAME;
				break;
			case "timestamp":
				serdeType = org.apache.hadoop.hive.metastore.ColumnType.TIMESTAMP_TYPE_NAME;
				break;
			case "timestamp with time zone":
				serdeType = org.apache.hadoop.hive.metastore.ColumnType.TIMESTAMPTZ_TYPE_NAME;
				break;
			case "tinyint":
				serdeType = org.apache.hadoop.hive.metastore.ColumnType.TINYINT_TYPE_NAME;
				break;
			case "uniontype":
				serdeType = org.apache.hadoop.hive.metastore.ColumnType.UNION_TYPE_NAME;
				break;
			case "utf8":
				serdeType = org.apache.hadoop.hive.metastore.ColumnType.STRING_TYPE_NAME;
				break;
			case "varchar":
				serdeType = org.apache.hadoop.hive.metastore.ColumnType.VARCHAR_TYPE_NAME;
				break;
			case "void":
				serdeType = org.apache.hadoop.hive.metastore.ColumnType.VOID_TYPE_NAME;
				break;
			default:
				if (!serdeType.startsWith("decimal(")) {
					// when in doubt, make it a string
					logger.info("Unable to find a column mapping for " + serdeType +
							".  Setting type to '" + org.apache.hadoop.hive.metastore.ColumnType.STRING_TYPE_NAME + "'.");
					serdeType = org.apache.hadoop.hive.metastore.ColumnType.STRING_TYPE_NAME;
				}
				// else serdeType is something like decimal(10,10)
				break;
			}
		}
		return serdeType;
	}

	/**
	 * 
	 * @param schema
	 * @return descriptions of the columns,
	 *  e.g. " (name string, age integer) "
	 * @throws ExplorerException
	 */
	List<FieldSchema> buildFieldDescriptors(ResultSetMetaData schema) throws ExplorerException {
		List<FieldSchema> ret = new ArrayList<FieldSchema>();

		try {
			int columnCount = schema.getColumnCount();
			logger.info("Schema:");
			
			if (columnCount > 0) {
				for (int i = 1; i < columnCount + 1; i++) {
					FieldSchema fs = new FieldSchema();
					String columnName = schema.getColumnName(i);
					fs.setName(columnName);
					String columnTypeName = schema.getColumnTypeName(i);
					String serdeType = translateToSerdeColumnType(columnTypeName);
					fs.setType(serdeType);
					logger.info(columnName + ":\t" + serdeType);
					ret.add(fs);
				}
				//desc.delete(desc.lastIndexOf(", "), desc.length());
			}
		} catch (SQLException e) {
			String msg = "Unable to process schema: " + e.getMessage();
			logger.log(Level.WARNING, msg, e);
			throw new ExplorerException(msg, e);
		}
		//desc.append(")");
		//String cols = desc.toString();
		return ret;
	}


}
