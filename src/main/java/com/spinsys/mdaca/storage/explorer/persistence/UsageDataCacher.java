package com.spinsys.mdaca.storage.explorer.persistence;

import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.persistence.Persistence;

import com.spinsys.mdaca.storage.explorer.model.DriveItem;
import com.spinsys.mdaca.storage.explorer.model.DriveQuery;
import com.spinsys.mdaca.storage.explorer.model.enumeration.DriveType;
import com.spinsys.mdaca.storage.explorer.provider.StorageProvider;
import com.spinsys.mdaca.storage.explorer.provider.StorageProviderFactory;
import com.spinsys.mdaca.storage.explorer.provider.WindowsStorageProvider;

/**
 * This class provides a background thread for collecting and saving
 * disk usage data.
 */
public class UsageDataCacher implements Runnable {
	
    private static final Logger logger =
			Logger.getLogger("com.spinsys.mdaca.storage.explorer.persistence.UsageDataCacher");
    
    private static final Logger sqlLogger = Logger.getLogger("org.hibernate.SQL");

	
	EntityManagerFactory emf;
    EntityManager entityManager;

	@Override
	public void run() {
		logger.info("Starting UsageDataCacher");
		
		try {
		    // We associate every record with the same start time to make it
		    // easy to identify which data is old and can be deleted. */
		    Date start = new Date(System.currentTimeMillis());
			emf = Persistence.createEntityManagerFactory(TableUtils.STOREXP_PERSISTENT_UNIT);
			entityManager = emf.createEntityManager();
			
			List<Drive> drives = entityManager.createQuery("from Drive", Drive.class).getResultList();
			collectAndSaveUsageData(drives, start);
			deleteOldFolderUsageData(drives);
		}
		catch (Exception e) {
			logger.log(Level.WARNING, "Starting UsageDataCacher", e);
		}
		finally {
			if (entityManager != null) {
				entityManager.close();
			}
		}
	}

	/**
	 * Get object disk usage data from the providers and save it to the database.
	 * @param drives the drives to collect data for
	 * @param start the time at which data collection started
	 */
	void collectAndSaveUsageData(List<Drive> drives, Date start) {
		logger.info("Starting to collect and save usage data");
		long totalBytes = 0L;

		DriveQuery query = getBaseQuery();

		for (Drive drive : drives) {
			DriveType driveType = drive.getDriveType();

			if (driveType != null) {
			    try {
			        StorageProvider provider = StorageProviderFactory.getProvider(driveType);
			        query.setDriveId(drive.getDriveId());

			        logger.info("Starting to collect usage data for " + drive);
			        totalBytes = provider.findAndSaveFileMemoryUsage(drive, entityManager, start);
			        DriveMemoryUsageHistory.saveUsageHistory(drive, totalBytes, entityManager);
			    } catch (Exception e) {
			        logger.log(Level.WARNING,
			                "Problem while collecting usage data for "
			                        + drive + ": " + e.getMessage(), e);
			    }
			}
		}
		logger.info("Finished saving usage data");
	}

	/**
	 * Save the usage data to the database.
     * @param drive the drive whose usage data will be saved
     * @param start the time at which data collection started
     * @param done true if this is the last of the data to be
     *   saved for drive, false otherwise
	 */
	public static long saveFileUsageDataAndClear(
	        EntityManager entityManager, Drive drive,
	        List<DriveItem> items, Date start, boolean done) {
		EntityTransaction transaction = null;
		long totalBytes = 0L;
		
		// This code would produce a lot of hibernate debug info, so
		// we temporarily set the hibernate log level to WARNING,
		// and restore it when done.
		Level sqlOrigLogLevel = sqlLogger.getLevel();
		sqlLogger.setLevel(Level.WARNING);

		try {
			transaction = entityManager.getTransaction();
			transaction.begin();

			totalBytes = saveFileUsageRecords(entityManager, drive, items, start, totalBytes);
			
			if (done) {
	            saveCompletionRecord(entityManager, drive, start);
			}
            // Clear the entityManager to reclaim memory from its cache
            entityManager.flush();
            entityManager.clear();

            transaction.commit();
		} catch (Exception e) {
			try {
				logger.log(Level.WARNING, e.getMessage(), e);

				if (transaction != null  && transaction.isActive()) {
					transaction.rollback();
				}
			} catch (Exception e1) {
				logger.log(Level.WARNING, "Exception during rollback", e);
			}
		}
		finally {
			// Restore the original log level
			sqlLogger.setLevel(sqlOrigLogLevel);
		}
		return totalBytes;
	}

    /**
     * Save the usage data to the database.
     * @param drive the drive whose usage data will be saved
     * @param start the time at which data collection started
     * @param done true if this is the last of the data to be
     *   saved for drive, false otherwise
     */
    public static long saveFileUsageDataInBatchesUsingIter(Drive drive, List<DriveItem> items,
            EntityManager entityManager, Date startDate)
    {
        long totalBytes = 0L;
        int batchSize = 1024;

        Iterator<DriveItem> iterator = items.iterator();
        long count = 0L;

        while (iterator.hasNext()) {
            DriveItem item = iterator.next();
            totalBytes += item.getFileSize();
            
            // Periodically save and clear out the items list to conserve memory
            if ((++count % batchSize) == 0) {
                UsageDataCacher.saveFileUsageDataAndClear(entityManager, drive, items, startDate, false);

//                if ((count % (50 * batchSize)) == 0) {
                    logger.info("Saved " + count + " items for " + drive);
//                }
            }
            // Shrink the items list to reclaim memory
            iterator.remove();
        }
        // Save any leftovers
        UsageDataCacher.saveFileUsageDataAndClear(entityManager, drive, items, startDate, true);
        logger.info("Saved a total of " + count + " items for " + drive);
        return totalBytes;
    }

    public static long saveFileUsageDataInBatches(Drive drive, List<DriveItem> items,
            EntityManager entityManager, Date startDate)
    {
        long totalBytes = 0L;
        
        // The number of items yet to be saved
        int origSize = items.size();
        int batchSize = 1024;
        int countSaved = 0;
        
        while (countSaved < origSize) {
            batchSize = Math.min(origSize - countSaved, batchSize);            
            totalBytes += UsageDataCacher.saveFileUsageDataAndClear(
                            entityManager, drive, items.subList(countSaved, countSaved + batchSize), startDate, false);
            countSaved += batchSize;
            logger.info("Saved " + countSaved + " items for " + drive);
        }
        // Save any leftovers
        UsageDataCacher.saveFileUsageDataAndClear(entityManager, drive, items, startDate, true);
        logger.info("Saved a total of " + countSaved + " items for " + drive);
        return totalBytes;
    }

    
	/**
     * Save the usage data to the database.
     * @param drive the drive whose usage data will be saved
     * @param start the time at which data collection started
     * @param done true if this is the last of the data to be
     *   saved for drive, false otherwise
	 */
    public static long saveFileUsageRecords(EntityManager entityManager,
            Drive drive, List<DriveItem> items, Date start,
            long totalBytes) throws Exception
    {
        // Save a record for every drive item
        for (DriveItem item : items) {
        	MemoryUsage usage = getBaseMemoryUsage(drive, start);
        	usage.setCreated(start);
        	usage.setPath(item.getPath());
        	long fileSize = item.getFileSize();
        	usage.setBytes(fileSize);
        	totalBytes += fileSize;
        	
        	try {
        	    entityManager.persist(usage);
        	}
        	catch (Exception ep) {
        	    logger.warning("Problem persisting " + item);
        	    throw ep;
        	}
        }
        return totalBytes;
    }
	
	/**
	 * Delete object usage data from the database.
	 * @param drives
	 */
	void deleteOldFolderUsageData(List<Drive> drives) {
		logger.info("Starting to delete old folder usage data");
		EntityTransaction transaction = null;

		for (Drive drive : drives) {
			DriveType driveType = drive.getDriveType();

			if (driveType != null) {
				try {
					transaction = entityManager.getTransaction();
					transaction.begin();
					int rowsDeleted = MemoryUsage.deleteOldFolderUsage(drive.getDriveId(), entityManager);
					logger.info("Deleted " + rowsDeleted + " rows of usage data for " +
								drive.getDisplayName());
					transaction.commit();
				} catch (Exception e) {
					try {
						logger.log(Level.WARNING, e.getMessage(), e);

						if (transaction != null && transaction.isActive()) {
							transaction.rollback();
						}
					} catch (Exception e1) {
						logger.log(Level.WARNING, "Exception during rollback", e);
					}
				}
			}
		}
		logger.info("Finished deleting old folder usage data");
	}


	/**
	 * Inserts a special record in the database indicating that this
	 * round of recording folder usage data has completed.
	 * @param drive
	 */
	public static void saveCompletionRecord(EntityManager entityManager,
	        Drive drive, Date start) {
		MemoryUsage markCompleted = getBaseMemoryUsage(drive, start);
		markCompleted.setCreated(start);
		markCompleted.setPath(MemoryUsage.COMPLETED);
		markCompleted.setBytes(0L);

		try {
            entityManager.persist(markCompleted);
        }
        catch (Exception ep) {
            logger.warning("Problem persisting " + markCompleted);
            throw ep;
        }
		logger.info("Completed storing folder usage data for " +
					drive.getDisplayName() + ".");
	}

	/**
	 * A convenience method for setting some of the data
	 * on the usage object
	 */
	private static MemoryUsage getBaseMemoryUsage(Drive drive, Date start) {
		MemoryUsage usage = new MemoryUsage();
		usage.setCreated(start);
		usage.setDrive(drive);
		return usage;
	}

	private DriveQuery getBaseQuery() {
		DriveQuery query = new DriveQuery();
		query.setRecursive(true);
		query.setSearchPattern(".*");
		query.setUsesPlaceholder(false);
		return query;
	}


}
