package com.spinsys.mdaca.storage.explorer.bigdata;

import java.io.File;
import java.io.IOException;
import java.util.logging.Logger;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;


public class AVROFileProcessor {

	private static final Logger logger =
            Logger.getLogger("com.spinsys.mdaca.storage.explorer.bigdata.AVROFileProcessor");


	// see https://stackoverflow.com/questions/45496786/how-to-extract-schema-from-an-avro-file-in-java
	public Schema getSchema(String filePath) throws IOException {
		Schema schema = null;
		DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
		File file = new File(filePath);
		try (DataFileReader<GenericRecord> dataFileReader =
				new DataFileReader<>(file, datumReader)) {
			schema = dataFileReader.getSchema();
		}
		logger.info("Avro schema for " + filePath + ": " + schema);
		return schema;
	}

}
