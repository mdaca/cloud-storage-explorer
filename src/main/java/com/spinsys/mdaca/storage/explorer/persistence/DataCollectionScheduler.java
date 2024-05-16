package com.spinsys.mdaca.storage.explorer.persistence;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;


@WebListener
public class DataCollectionScheduler implements ServletContextListener {

    private static final Logger logger =
            Logger.getLogger("com.spinsys.mdaca.storage.explorer.persistence.DataCollectionScheduler");

    private ScheduledExecutorService scheduler;

	public DataCollectionScheduler() {
	}

	@Override
	public void contextInitialized(ServletContextEvent sce) {
        scheduler = Executors.newSingleThreadScheduledExecutor();
        logger.info("Starting UsageDataCacher now");
        scheduler.scheduleWithFixedDelay(new UsageDataCacher(), 0, 60, TimeUnit.MINUTES);

        // TODO totally remove this and called code
//        logger.info("Starting DiskUsageHistoryDataCollector in 20 minutes");
//        scheduler.scheduleWithFixedDelay(new DiskUsageHistoryDataCollector(), 20, 60, TimeUnit.MINUTES);
	}

	@Override
	public void contextDestroyed(ServletContextEvent sce) {
        scheduler.shutdownNow();
	}
	
}
