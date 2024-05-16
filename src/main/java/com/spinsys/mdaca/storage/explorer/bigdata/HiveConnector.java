package com.spinsys.mdaca.storage.explorer.bigdata;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.thrift.TException;

import com.spinsys.mdaca.storage.explorer.model.exception.ExplorerException;
import com.spinsys.mdaca.storage.explorer.persistence.Drive;

import static com.spinsys.mdaca.storage.explorer.provider.CloudStorageProvider.HIVE_HOST_NAME;
import static com.spinsys.mdaca.storage.explorer.provider.CloudStorageProvider.HIVE_PORT;

public class HiveConnector implements AutoCloseable {

	private static final Logger logger =
			Logger.getLogger("com.spinsys.mdaca.storage.explorer.bigdata.HiveConnector");
	
	HiveMetaStoreClient metaStoreClient = null;


	public HiveConnector(Drive drive) throws ExplorerException {
		String host = drive.getPropertyValue(HIVE_HOST_NAME);
		
		if (host == null || "".equals(host)) {
			throw new ExplorerException("No HiveHostName property specified");
		}
		
		int port = 9083;
		String sHivePort = drive.getPropertyValue(HIVE_PORT);
		
		if (sHivePort != null) {
			port = Integer.parseInt(sHivePort);
		}
		else {
			logger.info("HivePort property wasn't set, defaulting to port " + port);
		}
		
		Configuration conf = new Configuration();
		String uri = "thrift://" + host + ":" + port;
		MetastoreConf.setVar(conf, MetastoreConf.ConfVars.THRIFT_URIS, uri);

		try {
			metaStoreClient = new HiveMetaStoreClient(conf);
//			metaStoreClient.setMetaConf("hive.orc.use-column-names", "true");
		} catch (TException e) {
			throw new ExplorerException(
					"Unable to open connection using URI (" +
					uri + "): " + e.getMessage(), e);
		}
	}

	public void createTable(Table tb) throws ExplorerException {
		try {
			metaStoreClient.createTable(tb);
		} catch (TException e) {
			String msg = "Unable to create table " + tb.getTableName() +
					": " + e.getMessage();
			logger.log(Level.WARNING, msg, e);
			throw new ExplorerException(msg, e);
		}
	}

	@Override
	public void close() throws Exception {
		logger.info("Closing hiveMetaStoreClient");
		metaStoreClient.close();
	}
	
	
}