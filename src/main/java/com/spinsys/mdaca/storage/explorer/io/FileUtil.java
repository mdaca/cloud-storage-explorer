package com.spinsys.mdaca.storage.explorer.io;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.net.URISyntaxException;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.logging.Logger;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.AgeFileFilter;
import org.apache.commons.io.filefilter.AndFileFilter;
import org.apache.commons.io.filefilter.OrFileFilter;
import org.apache.commons.io.filefilter.PrefixFileFilter;
import org.apache.commons.lang3.time.DateUtils;

public class FileUtil {

    public static final String MDACA_PREFIX = "MDACA_";

	public static final String PLACEHOLDER_FILE_NAME =
			"MDACA_FILEEXPLORER.txt";

	public static final String TMP_DIR_PROPERTY_VALUE =
			System.getProperty("java.io.tmpdir");

	public static final File TMP_DIR = new File(TMP_DIR_PROPERTY_VALUE);

	public static final Logger logger =
			Logger.getLogger("com.spinsys.mdaca.storage.explorer.io.FileUtil");


    /**
     * folders in some cloud services are not allowed to be empty
     * below is the name of a dummy file that will be placed to keep the folder from being deleted
     * this file will be skipped when collecting the names inside depending on the DriveQuery
     * DriveQuery.setUsesPlaceholder(false) -> do NOT include the placeholder file in the search
     */
    public static BasicFile buildPlaceholderFile() throws IOException {
        BasicFile placeholderFile = BasicFile.buildTempFile(PLACEHOLDER_FILE_NAME);

        placeholderFile.write("This file is leverage by CSE to manage the Azure directory structure. " +
                "Please do not delete this file.");
        return placeholderFile;
    }
    
    public static double getProcessCpuLoad() throws Exception {

        MBeanServer mbs    = ManagementFactory.getPlatformMBeanServer();
        ObjectName name    = ObjectName.getInstance("java.lang:type=OperatingSystem");
        AttributeList list = mbs.getAttributes(name, new String[]{ "ProcessCpuLoad" });

        if (list.isEmpty())     return Double.NaN;

        Attribute att = (Attribute)list.get(0);
        Double value  = (Double)att.getValue();

        // usually takes a couple of seconds before we get real values
        if (value == -1.0)      return Double.NaN;
        // returns a percentage value with 1 decimal point precision
        return ((int)(value * 1000) / 10.0);
    }
    
    /**
     * Retrive a file that is on the classpath
     * @param fileName the name of the file to retrieve
     * @return the first file found on the classpath, or
     * null if one doesn't exist.
     */
	public static File getResourceFile(String fileName) {
		BasicFile file = null;

		URL resource = FileUtil.class.getClassLoader().getResource(fileName);

		if (resource == null) {
			throw new IllegalArgumentException(fileName + " not found.");
		} else {

			// failed if files have whitespaces or special characters
			// return new File(resource.getFile());

			try {
				file = new BasicFile(resource.toURI());
			} catch (URISyntaxException e) {
				logger.severe(e.getMessage());
			}
		}
		return file;
	}
	
	public static BasicFile buildTempFile(String fileKey) throws IOException {
//		logger.info("getTempFile(" + fileKey + ")");
		java.io.File tempFile =
				File.createTempFile(MDACA_PREFIX + fileKey + "_", ".tmp");
		BasicFile basicFile = getTempBasicFile(tempFile);
		return basicFile;
	}

	protected static BasicFile getTempBasicFile(java.io.File tempFile) throws IOException {
		BasicFile basicFile = new BasicFile(tempFile.getCanonicalPath());

		/* We should delete the tempFile after done with it.
		 * The TempFileManager should delete the temp file from
		 * disk when the garbage collector sees no more references
		 * to the corresponding BasicFile Java object.
		 */
//		TempFileManager manager = TempFileManager.getInstance();
//		manager.deleteWhenUnused(basicFile);

		// deleteOnExit is an extra safeguard, but not to be relied upon.
		tempFile.deleteOnExit();
		return basicFile;
	}

	public static boolean deleteTempFile(File tempFile) {
		boolean wasDeleted = false;
		
		if (tempFile != null && tempFile.exists()) {
			wasDeleted = tempFile.delete();
			
			if (!wasDeleted) {
				logger.info("Unable to delete tempFile - " + tempFile);
			}
		}
		return wasDeleted;
	}
	
	/**
	 * Determine whether there is enough disk space available
	 * to create a file of the given size
	 * @param size the size of the file to be created in bytes
	 * @return true if there is enough space; false otherwise
	 */
	public static boolean spaceExists(long size) {
		boolean canMakeSpace = true;
        long usableSpace = TMP_DIR.getUsableSpace();
        long clearedSpace = 0; // the number of bytes recovered by deleting temp files
        int hourCutoff = 24; // delete temp file older than this
        
		// If there isn't enough space available,
        // delete older temp files to try and create
		// enough room.  Start deleting files older than a
		// day and keep dividing the interval by 2 until there is
		// enough space or we have no temp files older than an hour.
        while (((usableSpace + clearedSpace) < size)
        		&& canMakeSpace
        		&& hourCutoff >= 1) {
    		long tempFilesSize = getMdacaFilesSize(TMP_DIR);
    		
    		if (usableSpace + tempFilesSize >= size) {
    			clearedSpace += deleteTempFilesAfterMinutes(60 * hourCutoff);
    		}
    		else {
    			canMakeSpace = false;
    		}
    		hourCutoff /= 2;
        }
		return ((usableSpace + clearedSpace) >= size);
	}

	protected static long getMdacaFilesSize(File tmpDir) {
		long mdacaFilesSize = 0;
		PrefixFileFilter nameFilterL = new PrefixFileFilter(MDACA_PREFIX.toLowerCase());
		PrefixFileFilter nameFilterU = new PrefixFileFilter(MDACA_PREFIX.toUpperCase());
		OrFileFilter mdacaFilter =
				new OrFileFilter(nameFilterL, nameFilterU);

		Iterator<File> mdacaFiles =
				FileUtils.iterateFiles(tmpDir, mdacaFilter, null);

		while (mdacaFiles.hasNext()) {
		    File oldFile = mdacaFiles.next();
		    mdacaFilesSize += FileUtils.sizeOf(oldFile);
		}
		return mdacaFilesSize;
	}

	/**
	 * Deletes file that were created more than numMinutes ago
	 * @param numMinutes the number of minutes to subtract from the
	 * current tie
	 * @return the number of bytes restored by deleting files
	 */
	public static long deleteTempFilesAfterMinutes(int numMinutes) {
		long numBytesDeleted = 0;

	    Date oldestAllowed = DateUtils.addMinutes(new Date(), -1 * numMinutes); //subtract minutes from now
	    AgeFileFilter ageFilter = new AgeFileFilter(oldestAllowed);
		PrefixFileFilter nameFilterL = new PrefixFileFilter(MDACA_PREFIX.toLowerCase());
	    PrefixFileFilter nameFilterU = new PrefixFileFilter(MDACA_PREFIX.toUpperCase());
		AndFileFilter oldMdacaFilter =
	    		new AndFileFilter(ageFilter,
	    							new OrFileFilter(nameFilterL, nameFilterU));

	    Iterator<File> filesToDelete =
	    		FileUtils.iterateFiles(TMP_DIR, oldMdacaFilter, null);
	
    	logger.info("Deleting files more than " + numMinutes + " minutes old");

		// Delete the old MDACA files
	    while (filesToDelete.hasNext()) {
	        File oldFile = filesToDelete.next();
        	long oldSize = FileUtils.sizeOf(oldFile);
			boolean wasDeleted = FileUtils.deleteQuietly(oldFile);
	        
	        if (wasDeleted) {
				numBytesDeleted += oldSize;
	        	logger.info("Deleted " + oldFile.getName());
	        }
	        else {
	        	logger.info("Unable to delete " + oldFile.getName());
	        }
	    }
	    return numBytesDeleted;
	}

	/**
	 * Creates an easily sortable date/time string convenient
	 *  for use in file names
	 * @return a string containing the current date/time,
	 *     with increased granularity going left to right.
	 */
	public static String getTimestamp() {
		return new SimpleDateFormat("yyyyMMdd_HHmmss").format(new java.util.Date());
	}

	public static void transferBytes(InputStream in, OutputStream out, int size) throws IOException {
		byte[] buffer = new byte[size];
		int len;
		while ((len = in.read(buffer)) != -1) {
			out.write(buffer, 0, len);
		}
		out.flush();
	}


}
