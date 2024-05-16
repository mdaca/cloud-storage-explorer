package com.spinsys.mdaca.storage.explorer.bigdata;

import static com.spinsys.mdaca.storage.explorer.provider.AWSS3StorageProvider.ACCESS_KEY_PROPERTY_KEY;
import static com.spinsys.mdaca.storage.explorer.provider.AWSS3StorageProvider.ACCESS_SECRET_PROPERTY_KEY;
import static com.spinsys.mdaca.storage.explorer.provider.AWSS3StorageProvider.BUCKET_NAME_PROPERTY_KEY;
import static com.spinsys.mdaca.storage.explorer.provider.AWSS3StorageProvider.REGION_PROPERTY_KEY;
import static com.spinsys.mdaca.storage.explorer.provider.CloudStorageProvider.HIVE_HOST_NAME;
import static com.spinsys.mdaca.storage.explorer.provider.CloudStorageProvider.HIVE_PORT;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import org.apache.hadoop.hive.metastore.ColumnType;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.spinsys.mdaca.storage.explorer.io.FileUtil;
import com.spinsys.mdaca.storage.explorer.model.enumeration.DriveType;
import com.spinsys.mdaca.storage.explorer.model.exception.ExplorerException;
import com.spinsys.mdaca.storage.explorer.persistence.Drive;
import com.spinsys.mdaca.storage.explorer.rest.DriveService;

public class HiveConnectorIT {
	
	private static final String DB_NAME = "mp_java_test";

	public static final Logger logger =
			Logger.getLogger("com.spinsys.mdaca.storage.explorer.rest.DriveServiceIT");

	public HiveConnectorIT() {
	}
	
	public Drive buildDrive() {
		Drive drive = new Drive();
		drive.setDisplayName("Sample S3 Bucket");
		drive.setDriveType(DriveType.S3);
		drive.setDriveId(1);
		drive.addPropertyValue(ACCESS_KEY_PROPERTY_KEY,
				System.getenv(ACCESS_KEY_PROPERTY_KEY));
		drive.addPropertyValue(ACCESS_SECRET_PROPERTY_KEY,
				System.getenv(ACCESS_SECRET_PROPERTY_KEY));
		drive.addPropertyValue(BUCKET_NAME_PROPERTY_KEY,
				System.getenv(BUCKET_NAME_PROPERTY_KEY));
		drive.addPropertyValue(REGION_PROPERTY_KEY, "us-east-1");
		drive.addPropertyValue(HIVE_HOST_NAME, "10.13.31.114");
//		drive.addPropertyValue(HIVE_HOST_NAME, "10.13.13.37");
		drive.addPropertyValue(HIVE_PORT, "9083");
		return drive;
	}


	private Table buildTestTable() {
		Table tb = new Table();
		tb.setDbName(DB_NAME);
		String timestamp = FileUtil.getTimestamp();
		tb.setTableName("HiveConnectorIT_" + timestamp);
//		tb.setTableName("mp_test_table_2");
		StorageDescriptor sd = new StorageDescriptor();
		List<FieldSchema> fieldSchemas = new ArrayList<FieldSchema>();
		FieldSchema s1 = new FieldSchema();
		s1.setName("site_id");
		s1.setType(ColumnType.INT_TYPE_NAME);
		fieldSchemas.add(s1);
		FieldSchema s2 = new FieldSchema();
		s2.setName("row_id");
		s2.setType(ColumnType.STRING_TYPE_NAME);
		fieldSchemas.add(s2);
		sd.setCols(fieldSchemas);
		
		SerDeInfo serdeInfo = new SerDeInfo();
		serdeInfo.setSerializationLib("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe");
		sd.setSerdeInfo(serdeInfo);
		sd.setInputFormat("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat");
		sd.setOutputFormat("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat");
		sd.setLocation("s3a://mdaca-storage-1/test");

		tb.setSd(sd);
		tb.setTableType("EXTERNAL_TABLE");
		return tb;
	}


	@Disabled("Don't want to build an excessive number of tables." +
			"  No known automated way to clean up.")
	@Test
	// For this to work, the computer running the test must be
	// connected to the VPN, and the computer running the
	// Hive metastore must be available.
	public void testCreateTable() throws Exception {
		List<String> dbs = null;
		List<String> tables = null;
		String dbName = null;

		Drive drive = buildDrive();
		
		try (HiveConnector connector = new HiveConnector(drive)) {
			HiveMetaStoreClient hiveMetaStoreClient = connector.metaStoreClient;

			Table table = buildTestTable();
			connector.createTable(table);
			dbName = table.getDbName();
			String newTableName = table.getTableName();
			logger.info("Created table " + table + "\nnamed " + newTableName +
					" in database " + dbName);

			dbs = hiveMetaStoreClient.getAllDatabases();
			tables = hiveMetaStoreClient.getAllTables(dbName);
			logger.info("Databases = " + dbs +
					", tables in " + dbName + " = " + tables);

			try {
				Database db = hiveMetaStoreClient.getDatabase(dbName);
				logger.info("db = " + db);
			}
			catch (NoSuchObjectException e) {
				logger.warning("Could not get database " + dbName + ": " + e.getMessage());
			}
			
			try {
				connector.createTable(table);
				fail("A second attempt to create " + newTableName + " should fail.");
			}
			catch (ExplorerException e) {
				logger.info("Failed second attempt to create " + table +
							":\n" + e.getMessage());
				Throwable cause = e.getCause();
				assertNotNull(cause);
				assertTrue(cause instanceof AlreadyExistsException);
			}
		}
	}
	

}
