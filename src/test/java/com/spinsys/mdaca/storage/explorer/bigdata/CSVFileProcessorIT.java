package com.spinsys.mdaca.storage.explorer.bigdata;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.IOException;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.spinsys.mdaca.storage.explorer.io.FileUtil;

public class CSVFileProcessorIT {

	@Test
	public void testGetSchema() throws IOException, SQLException {
		File inputFile = FileUtil.getResourceFile("sizedFiles/stateCodes_u1kb.csv");
		String filePath = inputFile.getCanonicalPath();
		CSVFileProcessor processor = new CSVFileProcessor();
		ResultSetMetaData schema = processor.getSchema(filePath);
		assertEquals(3, schema.getColumnCount());
		assertEquals("idstate", schema.getColumnName(1));
		assertEquals("labelstate", schema.getColumnName(2));
		assertEquals("fullstatename", schema.getColumnName(3));
		assertEquals("integer", schema.getColumnTypeName(1).toLowerCase());
		assertEquals("string", schema.getColumnTypeName(2).toLowerCase());
		assertEquals("string", schema.getColumnTypeName(3).toLowerCase());
//		assertEquals("java.lang.String", schema.getColumnClassName(2));
	}

	@Disabled("Need to determine requirement for header line(s)")
	@Test
	public void testGetSchemaDiabetes() throws IOException, SQLException {
		// TODO put file into test/resources
		File inputFile = new File("S:\\Data\\CSV\\Diabetic\\diabetic_data_BI_Dataset.csv");
		String filePath = inputFile.getCanonicalPath();
		CSVFileProcessor processor = new CSVFileProcessor();
		ResultSetMetaData schema = processor.getSchema(filePath);
	}

	@Disabled("Need to implement separator detection")
	@Test
	public void testGetSchemaVertBar() throws IOException, SQLException {
		File inputFile = FileUtil.getResourceFile("csvFiles/s3providersVertBar.csv");
		String filePath = inputFile.getCanonicalPath();
		CSVFileProcessor processor = new CSVFileProcessor();
		ResultSetMetaData schema = processor.getSchema(filePath);
		assertEquals(12, schema.getColumnCount());
		assertEquals("Id", schema.getColumnName(1));
		assertEquals("ORGANIZATION", schema.getColumnName(2));
		assertEquals("ZIP", schema.getColumnName(9));
		assertEquals("string", schema.getColumnTypeName(1).toLowerCase());
		assertEquals("string", schema.getColumnTypeName(2).toLowerCase());
		assertEquals("integer", schema.getColumnTypeName(3).toLowerCase());
//		assertEquals("java.lang.String", schema.getColumnClassName(2));
	}

}
