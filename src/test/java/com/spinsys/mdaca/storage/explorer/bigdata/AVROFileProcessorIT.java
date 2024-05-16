package com.spinsys.mdaca.storage.explorer.bigdata;

import java.io.File;
import java.io.IOException;
import java.util.logging.Logger;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.junit.jupiter.api.Test;

import com.spinsys.mdaca.storage.explorer.io.FileUtil;

public class AVROFileProcessorIT extends PersonBuilder {

	private static final Logger logger =
            Logger.getLogger("com.spinsys.mdaca.storage.explorer.bigdata.AVROFileProcessorIT");


	@Test
	public void testGetTwitterSchema() throws Exception {
		File inputFile = FileUtil.getResourceFile("schemas/twitter.avro");
		String filePath = inputFile.getCanonicalPath();
		AVROFileProcessor processor = new AVROFileProcessor();
		Schema schema = processor.getSchema(filePath);
		logger.info(schema.toString());
	}
	

	/**
	 * Generate an AVRO file based on the ExportData class
	 * @param fileName
	 * @return
	 * @throws IOException
	 */
	// see https://stackoverflow.com/questions/11866466/using-apache-avro-reflect
	Schema writeAVRO(String fileName) throws IOException {
		Schema schema = ReflectData.get().getSchema(Person.class);
		File file = new File(fileName);
		DatumWriter<Person> writer = null;
		DataFileWriter<Person> dataFileWriter = null;
		try {
			writer = new ReflectDatumWriter<>(Person.class);
			dataFileWriter = new DataFileWriter<>(writer);
			
			dataFileWriter.setCodec(CodecFactory.deflateCodec(9))
		      .create(schema, file);

		    // write 10 records to the file
		    for (byte i = 0; i < 10; i++) {
		    	Person person = createPersonWithId(i);
		    	person.getMiscTraits().put("numberOfToes", "" + i);
				dataFileWriter.append(person);
		    }
		}
		finally {
			if (dataFileWriter != null) {
				dataFileWriter.close();
			}
		}
		logger.info("schema =\n" + schema);
		return schema;
	}

	@Test /* This isn't really a test.  It's just a convenient way
	 			to generate an AVRO file. */
	public void testWriteNestedAVRO() throws Exception {
		Schema schema = writeAVRO("C:/Temp/genPerson" + FileUtil.getTimestamp() + ".avro");
		logger.info(schema.toString());
	}
	

}
