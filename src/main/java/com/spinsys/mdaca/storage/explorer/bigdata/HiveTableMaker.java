package com.spinsys.mdaca.storage.explorer.bigdata;

import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.avro.Schema;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.hive.metastore.TableType;
//import org.apache.hadoop.hive.metastore.ColumnType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
//import org.apache.parquet.schema.PrimitiveType;
//import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.MessageType;

import com.spinsys.mdaca.storage.explorer.io.PathProcessor;
import com.spinsys.mdaca.storage.explorer.model.exception.ExplorerException;

public class HiveTableMaker {
	
	private static final Logger logger =
            Logger.getLogger("com.spinsys.mdaca.storage.explorer.bigdata.HiveTableMaker");



	/**
	 * Creates an external table to import data from a CSV
	 * file on a file system, into Hive.
	 * @param hiveTableName the table name that Hive will use
	 * @param location specifies the location of external data
	 * @param metaData the schema to use for the external data definition
	 * @param the field separator for the table values,
	 *  typically a comma
	 */
	public Table createExternalCSVTable(
			String databaseName, 
			String hiveTableName,
			String location,
			ResultSetMetaData metaData, char fieldSep) throws ExplorerException {
		Table table = createExternalTable(databaseName, hiveTableName);
		addCSVDescriptor(table, metaData, location, fieldSep);
		return table;
	}

	/**
	 * @see https://cwiki.apache.org/confluence/display/Hive/AvroSerDe#AvroSerDe-SpecifyingtheAvroschemaforatable
	 * @param hiveTableName
	 * @param location
	 * @param schema 
	 * @param fieldSep
	 * @return
	 */
	public Table createExternalAVROTable(
			String sourcePath,
			String databaseName, 
			String hiveTableName,
			String location,
			Schema schema, char fieldSep) {
//		String json = schema.toString();
		Table result = createExternalAVROTableUsingAVSC(sourcePath, databaseName, hiveTableName, location);
//		Table result = createExternalAVROTableUsingJSONSchema(databaseName, hiveTableName, json, location);
		// see https://community.cloudera.com/t5/Support-Questions/how-to-create-and-store-the-avro-files-in-hive-table/td-p/139649
		// see https://cwiki.apache.org/confluence/display/Hive/AvroSerDe
		return result;
	}
	
	/**
	 * 
	 * @param hiveTableName the name of the Hive table to be created
	 * @param schemaJSON the JSON representation of the table schema
	 * @param location e.g., 's3://public-qubole/datasets/avro/episodes'
	 * @return A HiveQL statement for creating an external table
	 * 	using the supplied schema
	 * @see https://docs.qubole.com/en/latest/user-guide/engines/hive/hive-table-formats/avro.html
	 */
	Table createExternalAVROTableUsingAVSC(
			String sourcePath,
			String databaseName, 
			String hiveTableName,
//			String schemaJSON,
			String location)
	{
		String avscLocation = getAVSCLocation(sourcePath, location);
		Table table = createExternalTable(databaseName, hiveTableName);
		// see https://www.programcreek.com/java-api-examples/?class=org.apache.hadoop.hive.metastore.api.Table&method=setTableType
		table.putToParameters("avro.schema.url", avscLocation);
//		TBLPROPERTIES ('avro.schema.url'='hdfs:///user/root/avro/schema/user.avsc');
//		  table.putToParameters(AVRO_SCHEMA_URL, schemaUri.toString());
//		  table.putToParameters(AVRO_SCHEMA_VERSION, Integer.toString(version));
		table.putToParameters("hive.serialization.extend.nesting.levels", "true");
		table.putToParameters("hive.serialization.extend.additional.nesting.levels", "true");
		
		addAVRODescriptor(table, location);
		return table;
	}

	void addAVRODescriptor(Table table, String location) {
		StorageDescriptor descriptor = new StorageDescriptor();
		table.setSd(descriptor);
		  
		// see https://cwiki.apache.org/confluence/display/Hive/AvroSerDe
		SerDeInfo serdeInfo = new SerDeInfo();
		serdeInfo.setSerializationLib("org.apache.hadoop.hive.serde2.avro.AvroSerDe");

		descriptor.setCols(new ArrayList<FieldSchema>());
		descriptor.setSerdeInfo(serdeInfo);
		descriptor.setInputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat");
	    descriptor.setOutputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat");
	    
	    // TODO It looks like the location specified by setLocation
	    // overrides the one specified by
	    //   table.putToParameters("avro.schema.url", avscLocation)
	    // We need to do some checking to see whether that is the case
	    descriptor.setLocation(location);
	}

	String getAVSCLocation(String sourcePath, String location) {
		// Assume the .avsc file is one directory above the .avro file
		// and shares the same base name.  For example, for a sourcePath of
		// dir1/dir2/baseName.avro
		// dir1/baseName.avsc contains the schema.
		String baseName = FilenameUtils.getBaseName(sourcePath);
		String parentLoc = PathProcessor.getParentFolder(PathProcessor.removeLastSlash(location));
		String avscLocation = PathProcessor.addLastSlash(parentLoc) + baseName + ".avsc";
		// e.g. - s3a://mdaca-storage-1/SchemaFiles/AVRO/twitterAVSC/twitter.avsc
		return avscLocation;
	}

	Table createExternalAVROTableUsingJSONSchema(
			String databaseName, 
			String hiveTableName,
			String schemaJSON,
			String location)
	{
		Table table = createExternalTable(databaseName, hiveTableName);
		table.putToParameters("avro.schema.literal", schemaJSON);
		table.putToParameters("hive.serialization.extend.additional.nesting.levels", "true");
		
		StorageDescriptor descriptor = new StorageDescriptor();
		table.setSd(descriptor);
		  
		// see https://cwiki.apache.org/confluence/display/Hive/AvroSerDe
		SerDeInfo serdeInfo = new SerDeInfo();
		Map<String, String> params = new HashMap<String, String>();
		params.put("EXTERNAL", "TRUE");
		params.put("avro.schema.literal", schemaJSON);
		serdeInfo.setParameters(params);
		serdeInfo.setSerializationLib("org.apache.hadoop.hive.serde2.avro.AvroSerDe");

		descriptor.setCols(new ArrayList<FieldSchema>());
		descriptor.setSerdeInfo(serdeInfo);		
	    descriptor.setInputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat");
	    descriptor.setOutputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat");
	    descriptor.setLocation(location);
		return table;
	}

	public Table createExternalParquetTable(String databaseName, String hiveTableName, String location,
			MessageType schema) throws ExplorerException {
		Table table = createExternalTable(databaseName, hiveTableName);
		addParquetDescriptor(table, schema, location);
		return table;
	}

	void addParquetDescriptor(Table table, MessageType schema, String location) throws ExplorerException {
		StorageDescriptor descriptor = new StorageDescriptor();

		if (!descriptor.isSetParameters()) {
			descriptor.setParameters(new HashMap<String, String>());
		}
		ParquetFileProcessor processor = new ParquetFileProcessor();
		List<FieldSchema> fieldSchemas = processor.buildFieldDescriptors(schema);
		descriptor.setCols(fieldSchemas);
		descriptor.setLocation(location);

		SerDeInfo serdeInfo = new SerDeInfo();
		serdeInfo.setSerializationLib("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe");
		descriptor.setSerdeInfo(serdeInfo);
		descriptor.setInputFormat("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat");
		descriptor.setOutputFormat("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat");
		table.setSd(descriptor);
	}

	Table addCSVDescriptor(Table table, ResultSetMetaData schema, String location, char fieldSep) throws ExplorerException {
		StorageDescriptor descriptor = new StorageDescriptor();

		if (!descriptor.isSetParameters()) {
			descriptor.setParameters(new HashMap<String, String>());
		}
		CSVFileProcessor processor = new CSVFileProcessor();
		List<FieldSchema> fieldSchemas = processor.buildFieldDescriptors(schema);
		descriptor.setCols(fieldSchemas);
		descriptor.setLocation(location);
		descriptor.setInputFormat("org.apache.hadoop.mapred.TextInputFormat");
		descriptor.setOutputFormat("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat");

		SerDeInfo serdeInfo = new SerDeInfo();
		// see https://cwiki.apache.org/confluence/display/Hive/CSV+Serde
		// *** This SerDe treats all columns to be of type String ***
		serdeInfo.setSerializationLib("org.apache.hadoop.hive.serde2.OpenCSVSerde");

		Map<String, String> parametersMap = new HashMap<>();
		parametersMap.put("serialization.format", "" + fieldSep);
		parametersMap.put("field.delim", "" + fieldSep);
		parametersMap.put("line.delim", "\n");
		serdeInfo.setParameters(parametersMap);
		descriptor.setSerdeInfo(serdeInfo);
		table.setSd(descriptor);
		return table;
	}
	
	Table createExternalTable(String databaseName, String hiveTableName) {
		Table table = new Table();
		table.setDbName(databaseName);
		table.setTableName(hiveTableName);
		table.setTableType(TableType.EXTERNAL_TABLE.toString());
		table.putToParameters("EXTERNAL", "TRUE");
		return table;
	}

	
}
