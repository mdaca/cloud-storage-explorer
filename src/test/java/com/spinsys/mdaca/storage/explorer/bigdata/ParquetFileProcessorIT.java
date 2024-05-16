package com.spinsys.mdaca.storage.explorer.bigdata;

import com.spinsys.mdaca.storage.explorer.io.FileUtil;
import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.logging.Logger;

/** Many of the "test" methods in this class aren't really tests.
 *  They provide a convenient way to generate Parquet
 *  files with known contents. */
public class ParquetFileProcessorIT extends PersonBuilder {
	
	private static final Logger logger = Logger
			.getLogger("com.spinsys.mdaca.storage.explorer.bigdata.ParquetFileProcessorIT");


	ParquetFileProcessor processor = new ParquetFileProcessor();

	@Test
	public void testGetParquetSchemaPrimitive() throws Exception {
		File file = FileUtil.getResourceFile("schemas/part-r-aa6d7.snappy.parquet");
		String filePath = file.getCanonicalPath();
		MessageType metaData = processor.getSchema(filePath);
		int fieldCount = metaData.getFieldCount();
		for (int i = 0; i < fieldCount; i++) { // column numbering starts at 1, not 0
			System.out.println(
					"field: " + i +
					", name: " + metaData.getFieldName(i) +
					", typeName: " + metaData.getType(i).getName() +
					", type: " + metaData.getType(i));
		}
	}

	@Disabled("In work")
	@Test
	public void testGetParquetSchemaComplex() throws Exception {
		File file = new File("S:\\Data\\Parquet\\mdacaDemo\\Inpatient\\inpatient_claims.parquet");
		String filePath = file.getCanonicalPath();
		MessageType metaData = processor.getSchema(filePath);
		int fieldCount = metaData.getFieldCount();
		for (int i = 0; i < fieldCount; i++) { // column numbering starts at 1, not 0
			System.out.println(
					"field: " + i +
					", name: " + metaData.getFieldName(i) +
					", typeName: " + metaData.getType(i).getName() +
					", type: " + metaData.getType(i));
		}
	}

	@Disabled("This isn't really a test.  It's just a convenient way "
			+ "	to generate a Parquet file with various kinds of numeric data.")
	@Test
	public void testWriteNumericParquet() throws IOException {
		Schema schema = ReflectData.AllowNull.get().getSchema(NumericData.class);
		String tableName = "genNumericData" + FileUtil.getTimestamp();
	    final String parquetFile = "C:/Temp/" + tableName + ".parquet";
	    final Path path = new Path(parquetFile);

	    Configuration conf = new Configuration();
	    conf.setBoolean(AvroReadSupport.AVRO_COMPATIBILITY, true);

	    try (ParquetWriter<NumericData> writer =
	    		AvroParquetWriter.<NumericData>builder(path)
	    	     .withSchema(schema)
	    	     .withDataModel(ReflectData.get())
	    	     .withConf(conf)
	    	     .withCompressionCodec(CompressionCodecName.SNAPPY)
	//    	     .withWriteMode(OVERWRITE)
	    	     .build())
	    {

		    // write 10 records to the file
		    for (byte i = 0; i < 10; i++) {
		    	writer.write(new NumericData());
		    }
	    }

//	    addToBDV("mp_java_test", tableName, parquetFile);
	}


	@Disabled("This isn't really a test.  It's just a convenient way "
			+ "	to generate a Parquet file with 10 Persons.")
	@Test
    public void testWriteFlatParquet() throws IOException {
		Schema schema = ReflectData.AllowNull.get().getSchema(FlatPerson.class);
        final String parquetFile = "C:/Temp/genFlatPerson" + FileUtil.getTimestamp() + ".parquet";
        final Path path = new Path(parquetFile);

        Configuration conf = new Configuration();
        conf.setBoolean(AvroReadSupport.AVRO_COMPATIBILITY, true);

        try (ParquetWriter<FlatPerson> writer =
        		AvroParquetWriter.<FlatPerson>builder(path)
        	     .withSchema(schema)
        	     .withDataModel(ReflectData.get())
        	     .withConf(conf)
        	     .withCompressionCodec(CompressionCodecName.SNAPPY)
//        	     .withWriteMode(OVERWRITE)
        	     .build())
        {

    	    // write 10 records to the file
    	    for (byte i = 0; i < 10; i++) {
    	    	FlatPerson person = createFlatPersonWithId(i);
    	    	writer.write(person);
    	    }
        }
    }

	@Disabled("This isn't really a test.  It's just a convenient way "
			+ "	to generate a Parquet file with 10 Persons.")
	@Test
	public void testWritePersonTraitsParquet() throws IOException {
		Schema schema = ReflectData.AllowNull.get().getSchema(PersonWithTraits.class);
	    final String parquetFile = "C:/Temp/genPersonTraits" + FileUtil.getTimestamp() + ".parquet";
	    final Path path = new Path(parquetFile);
	
	    Configuration conf = new Configuration();
	    conf.setBoolean(AvroReadSupport.AVRO_COMPATIBILITY, true);
	
	    try (ParquetWriter<PersonWithTraits> writer =
	    		AvroParquetWriter.<PersonWithTraits>builder(path)
	    	     .withSchema(schema)
	    	     .withDataModel(ReflectData.get())
	    	     .withConf(conf)
	    	     .withCompressionCodec(CompressionCodecName.SNAPPY)
	//    	     .withWriteMode(OVERWRITE)
	    	     .build())
	    {
	
		    // write 10 records to the file
		    for (byte i = 0; i < 10; i++) {
		    	PersonWithTraits person = createPersonWithTraits(i);
		    	writer.write(person);
		    }
	    }
	}

	@Disabled("This isn't really a test.  It's just a convenient way "
			+ "	to generate a Parquet file with 10 Persons.")
	@Test
	public void testWritePersonParquet() throws IOException {
		Schema schema = ReflectData.AllowNull.get().getSchema(Person.class);
	    final String parquetFile = "C:/Temp/genPerson" + FileUtil.getTimestamp() + ".parquet";
	    final Path path = new Path(parquetFile);
	
	    Configuration conf = new Configuration();
	    conf.setBoolean(AvroReadSupport.AVRO_COMPATIBILITY, true);
	
	    try (ParquetWriter<Person> writer =
	    		AvroParquetWriter.<Person>builder(path)
	    	     .withSchema(schema)
	    	     .withDataModel(ReflectData.get())
	    	     .withConf(conf)
	    	     .withCompressionCodec(CompressionCodecName.SNAPPY)
	//    	     .withWriteMode(OVERWRITE)
	    	     .build())
	    {
	
		    // write 10 records to the file
		    for (byte i = 0; i < 10; i++) {
		    	Person person = createPerson(i);
		    	writer.write(person);
		    }
	    }
	}

	@Disabled("This isn't really a test.  It's just a convenient way "
			+ "	to generate a Parquet file with 10 Persons.")
	@Test
	public void testWriteNestedParquet() throws IOException {
		Schema schema = ReflectData.AllowNull.get().getSchema(Person.class);
	    final String parquetFile = "C:/Temp/genNestedPerson" + FileUtil.getTimestamp() + ".parquet";
	    final Path path = new Path(parquetFile);

	    Configuration conf = new Configuration();
	    conf.setBoolean(AvroReadSupport.AVRO_COMPATIBILITY, true);

	    try (ParquetWriter<Person> writer =
	    		AvroParquetWriter.<Person>builder(path)
	    	     .withSchema(schema)
	    	     .withDataModel(ReflectData.get())
	    	     .withConf(conf)
	    	     .withCompressionCodec(CompressionCodecName.SNAPPY)
	//    	     .withWriteMode(OVERWRITE)
	    	     .build())
	    {

		    // write 10 records to the file
		    for (byte i = 0; i < 10; i++) {
		    	Person person = createPersonWithId(i);
		    	person.getMiscTraits().put("numberOfToes", "" + i);
		        writer.write(person);
		    }
	    }
	}


//	void addToBDV(String dbName, String tableName, String fileName) {
//		ParquetFileProcessor processor = new ParquetFileProcessor();
//		MessageType schema = processor.getSchema(fileName);
//
//		HiveTableMaker hive = new HiveTableMaker();
//		String location = provider.getHiveLocationPath(drive, path);
//		Table tableDef = hive.createExternalParquetTable(dbName, tableName, location, schema);
//
//		try (HiveConnector connector = new HiveConnector(drive)) {
//			connector.createTable(tableDef);
//		}
//
//	}
}
