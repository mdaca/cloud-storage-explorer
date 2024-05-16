package com.spinsys.mdaca.storage.explorer.bigdata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.ColumnType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

import com.spinsys.mdaca.storage.explorer.model.exception.ExplorerException;

public class ParquetFileProcessor {
	
    private static final Logger logger =
            Logger.getLogger("com.spinsys.mdaca.storage.explorer.bigdata.ParquetFileProcessor");

	public MessageType getSchema(String filePath) throws ExplorerException {
		Configuration conf = new Configuration();
		HadoopInputFile hadoopFile;
		try {
			hadoopFile = HadoopInputFile.fromPath(new Path(filePath), conf);
		} catch (IllegalArgumentException | IOException e) {
			String msg = "Unable to get Hadoop file from " + filePath + ": " + e.getMessage();
			logger.log(Level.WARNING, msg, e);
			throw new ExplorerException(msg, e);
		}
		MessageType schema = getSchemaFromHadoopFile(hadoopFile);
		return schema;
	}
	
	MessageType getSchemaFromHadoopFile(HadoopInputFile hadoopFile) throws ExplorerException {
		try (ParquetFileReader reader = ParquetFileReader.open(hadoopFile)) {
			MessageType schema = getSchema(reader);
			return schema;
		} catch (IOException e) {
			String msg = "Unable to get schema from Hadoop file from " + hadoopFile + ": " + e.getMessage();
			logger.log(Level.WARNING, msg, e);
			throw new ExplorerException(msg, e);
		}
	}

	static MessageType getSchema(ParquetFileReader reader) {
		FileMetaData metaData = reader.getFooter().getFileMetaData();
		MessageType schema = metaData.getSchema();
		return schema;
	}

	public static String extractTypeString(PrimitiveType primitiveType) {
		// e.g. "optional binary SITE_ID (STRING)"
		//      "optional binary ENCOUNTER_ID (DECIMAL(10,10))"
		// see https://javadoc.io/doc/org.apache.parquet/parquet-column/1.9.0/org/apache/parquet/schema/PrimitiveType.PrimitiveTypeName.html
		PrimitiveTypeName primitiveTypeName = primitiveType.getPrimitiveTypeName();
		String sPrimType = primitiveType.toString();
		int indexLparen = sPrimType.indexOf("(");
		int indexRparen = sPrimType.lastIndexOf(")");
		
		String fieldType =
				(indexLparen >= 0 && indexLparen < indexRparen)
				? sPrimType.substring(indexLparen + 1, indexRparen) // e.g., DECIMAL(10,10)
				: primitiveTypeName.name(); // e.g. "STRING"
		return fieldType;
	}
	
	/**
	 * 
	 * @param groupSchema
	 * @return descriptions of the columns,
	 *  e.g. " (name string, age integer) "
	 * @throws ExplorerException
	 */
	List<FieldSchema> buildFieldDescriptors(GroupType groupSchema) throws ExplorerException {
		List<FieldSchema> fieldSchemas = new ArrayList<FieldSchema>();

		int fieldCount = groupSchema.getFieldCount();
		logger.info("Schema:");
		
		if (fieldCount > 0) {
			for (int i = 0; i < fieldCount; i++) {
				Type fieldType = groupSchema.getType(i);
				String fieldName = groupSchema.getFieldName(i);
				FieldSchema fieldSchema = buildFieldSchema(fieldType, fieldName);
				fieldSchemas.add(fieldSchema);
			}
		}
		return fieldSchemas;
	}

	FieldSchema buildFieldSchema(Type fieldType, String fieldName) throws ExplorerException {
		FieldSchema fieldSchema = new FieldSchema();
		fieldSchema.setName(fieldName);
		
		if (fieldType.isPrimitive()) {
			fieldSchema = buildPrimitiveSchema(fieldType, fieldSchema, fieldName);
		}
		// group type - see https://www.javadoc.io/doc/org.apache.parquet/parquet-column/1.8.1/org/apache/parquet/schema/GroupType.html
		// per http://hadooptutorial.info/hive-data-types-examples/#Complex_Data_Types,
		// Hive supports four complex data types - ARRAY, MAP, STRUCT, UNIONTYPE
		// https://www.javadoc.io/static/org.apache.parquet/parquet-column/1.8.1/org/apache/parquet/schema/OriginalType.html
		//   includes BSON, JSON, LIST, MAP
		// https://www.javadoc.io/doc/org.apache.parquet/parquet-column/1.8.1/org/apache/parquet/schema/GroupType.html
		//   GroupType(Type.Repetition repetition, String name, Type... fields) 
		else {
			GroupType fieldGroupType = fieldType.asGroupType();
			LogicalTypeAnnotation logicalType = fieldGroupType.getLogicalTypeAnnotation();

//					// don't rely on deprecated code
//					OriginalType originalType = groupType.getOriginalType();
//					if (OriginalType.LIST.equals(originalType)) {}
//					else if (OriginalType.MAP.equals(originalType)) {} // etc

			if (LogicalTypeAnnotation.listType().equals(logicalType)) {
				fieldSchema = buildListSchema(fieldGroupType, fieldSchema);
			}
			else if (LogicalTypeAnnotation.mapType().equals(logicalType)) {
				// List type seems to have a single child - MAP_KEY_VALUE
				fieldSchema = buildMapSchema(fieldGroupType, fieldSchema);
			}
			// TODO union
			else { // assume struct
				fieldSchema = buildStructSchema(fieldGroupType, fieldSchema);
			}
		}
		logger.info(fieldName + " schema:\t" + fieldSchema);
		return fieldSchema;
	}

	FieldSchema buildPrimitiveSchema(Type fieldType, FieldSchema fieldSchema, String fieldName) {
		String typeName = extractTypeString(fieldType.asPrimitiveType());
		String serdeType = translateToSerdeColumnType(typeName);
		fieldSchema.setType(serdeType);
		return fieldSchema;
	}

	/**
	 * 
	 * @param listGroupType, e.g.,
	       optional group randomNumbers (LIST) {
  			  repeated int32 array;
		   }
	 * @param fieldSchema
	 * @return the fieldSchema updated with the type info
	 * @throws ExplorerException 
	 */
	FieldSchema buildListSchema(GroupType listGroupType, FieldSchema fieldSchema) throws ExplorerException {
		List<Type> fields = listGroupType.getFields();
		if (listGroupType.getFieldCount() > 0) {

			// expecting 1 field: e.g., repeated int32 array
			Type fieldType = fields.get(0);
			FieldSchema contentSchema = buildFieldSchema(fieldType, null);

			// generate the type, e.g., list<string>
			StringBuilder builder = new StringBuilder(ColumnType.LIST_TYPE_NAME);
			builder.append("<");
			builder.append(contentSchema.getType());
			builder.append(">");
			String type = builder.toString();

			fieldSchema.setType(type);
		}
		else {
			logger.warning("No fields indicating the List type of " + listGroupType);
		}
		return fieldSchema;
	}

	FieldSchema buildMapSchema(GroupType mapGroupType, FieldSchema fieldSchema) {
		List<Type> fields = mapGroupType.getFields();
		if (mapGroupType.getFieldCount() > 0) {
			// expecting 1 MAP_KEY_VALUE field
			/*
			   repeated group key_value (MAP_KEY_VALUE) {
					required binary key (STRING);
					required binary value (STRING);
				}
			 */
			Type field0 = fields.get(0);
			
			// A map isn't primitive
			if (!field0.isPrimitive()) {
				GroupType keyValueType = field0.asGroupType();
				List<Type> kvFields = keyValueType.getFields();
				// expecting 2 fields - one key, one value
				if (kvFields.size() == 2) {
					Type keyType = kvFields.get(0);
					String keyTypeS = keyType.getOriginalType().name();
					String keySerdeType = translateToSerdeColumnType(keyTypeS);

					Type valType = kvFields.get(1);
					String valTypeS = valType.getOriginalType().name();
					String valSerdeType = translateToSerdeColumnType(valTypeS);

					 // generate the type, e.g., map<string,string>
					StringBuilder builder = new StringBuilder(ColumnType.MAP_TYPE_NAME);
					builder.append("<");
					builder.append(keySerdeType).append(",");
					builder.append(valSerdeType);
					builder.append(">");
					String complexType = builder.toString();
					fieldSchema.setType(complexType);
				}
				else {
					logger.warning("Expected 2 map fields for " + field0 +
							", but found " + kvFields.size() + ".");
				}
			}
			else {
				logger.warning("Alleged map field " + field0 + " is a primitive type.");
			}
		}
		else {
			logger.warning("No subfields found for " + mapGroupType + ".");
		}
		return fieldSchema;
	}

	FieldSchema buildStructSchema(GroupType fieldGroupType, FieldSchema fieldSchema)
			throws ExplorerException
	{
		List<FieldSchema> subFields = buildFieldDescriptors(fieldGroupType);
		StringBuilder builder = new StringBuilder(ColumnType.STRUCT_TYPE_NAME);
		builder.append("<");
		
		for (FieldSchema sub : subFields) {
			builder.append(sub.getName()).append(":");
			builder.append(sub.getType()).append(",");
		}
		builder.deleteCharAt(builder.lastIndexOf(","));
		builder.append(">");
		String complexType = builder.toString();
		fieldSchema.setType(complexType);
		return fieldSchema;
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
		String serdeType = ColumnType.STRING_TYPE_NAME;
		if (columnTypeName != null) {
			serdeType = columnTypeName.toLowerCase();

			// see https://www.javadoc.io/doc/org.apache.parquet/parquet-column/1.8.1/org/apache/parquet/schema/OriginalType.html
			// TODO check the various date, time conversions,
			// particularly relative to the local time zone.
			switch (serdeType) {
			case "binary":
				// TODO - how does text/string data relate to binary
				serdeType = ColumnType.BINARY_TYPE_NAME;
				break;
			case "boolean":
				serdeType = ColumnType.BOOLEAN_TYPE_NAME;
				break;
//			case "bson":
//				// TODO ???
//				break;
			case "date":
				serdeType = ColumnType.DATE_TYPE_NAME;
				break;
			case "double":
				serdeType = ColumnType.DOUBLE_TYPE_NAME;
				break;
			case "fixed_len_byte_array":
				serdeType = ColumnType.LIST_TYPE_NAME;
				break;
			case "float":
				serdeType = ColumnType.FLOAT_TYPE_NAME;
				break;
			case "int_8":
			case "int8":
				serdeType = ColumnType.TINYINT_TYPE_NAME;
				break;
			case "int_16":
			case "int16":
				serdeType = ColumnType.SMALLINT_TYPE_NAME;
				break;
			case "int":
			case "integer":
			case "int_32":
			case "int32":
				serdeType = ColumnType.INT_TYPE_NAME;
				break;
			case "int64":
				serdeType = ColumnType.BIGINT_TYPE_NAME;
				break;
			case "int96": // TODO check
				serdeType = ColumnType.TIMESTAMP_TYPE_NAME;
				break;
			case "interval": // TODO check
				serdeType = ColumnType.INTERVAL_DAY_TIME_TYPE_NAME;
//				serdeType = ColumnType.INTERVAL_YEAR_MONTH_TYPE_NAME;
				break;
//				case "json":
//				// TODO
//				break;
			case "list":
				serdeType = ColumnType.LIST_TYPE_NAME;
				break;
			case "map":
				serdeType = ColumnType.MAP_TYPE_NAME;
				break;
//			case "map_key_value":
//				// TODO - currently handled within the
				// buildMapSchema() MAP processing
//				break;
			case "string":
				serdeType = ColumnType.STRING_TYPE_NAME;
				break;
			case "struct":
				serdeType = ColumnType.STRUCT_TYPE_NAME;
				break;
			case "text":
				serdeType = ColumnType.STRING_TYPE_NAME;
				break;
			case "timestamp":
			case "timestamp_micros": // TODO check
			case "timestamp_millis": // TODO check
				serdeType = ColumnType.TIMESTAMP_TYPE_NAME;
				break;
			case "uint_8":
				serdeType = ColumnType.TINYINT_TYPE_NAME;
				break;
			case "uint_16":
				serdeType = ColumnType.SMALLINT_TYPE_NAME;
				break;
			case "uint_32":
				serdeType = ColumnType.INT_TYPE_NAME;
				break;
			case "uint_64":
				serdeType = ColumnType.BIGINT_TYPE_NAME;
				break;
			case "union": // TODO check
			case "uniontype": // TODO check
				serdeType = ColumnType.UNION_TYPE_NAME;
				break;
			case "utf8":
				serdeType = ColumnType.STRING_TYPE_NAME;
				break;
			default:
				if (!serdeType.startsWith("decimal(")) {
					// when in doubt, make it a string
					logger.info("Unable to find a column mapping for " + serdeType +
							".  Setting type to '" + ColumnType.STRING_TYPE_NAME + "'.");
					serdeType = ColumnType.STRING_TYPE_NAME;
				}
				// else serdeType is something like decimal(10,10)
				break;
			}
		}
		return serdeType;
	}


}
