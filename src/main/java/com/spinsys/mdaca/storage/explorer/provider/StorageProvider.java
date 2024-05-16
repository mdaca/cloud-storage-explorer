package com.spinsys.mdaca.storage.explorer.provider;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.List;

import javax.persistence.EntityManager;

import com.spinsys.mdaca.storage.explorer.io.BasicFile;
import com.spinsys.mdaca.storage.explorer.model.DriveItem;
import com.spinsys.mdaca.storage.explorer.model.DriveQuery;
import com.spinsys.mdaca.storage.explorer.model.dto.DriveMemoryUsageDTO;
import com.spinsys.mdaca.storage.explorer.model.exception.ExplorerException;
import com.spinsys.mdaca.storage.explorer.persistence.Drive;

/**
 * @author Keith Cassell
 *
 */
public interface StorageProvider {

	boolean testConnection(Drive drive) throws ExplorerException;

	List<DriveItem> find(Drive drive, DriveQuery query) throws ExplorerException;

	BasicFile download(Drive drive, String path) throws IOException;

	void upload(Drive drive, String path, File file) throws IOException;

	void copy(Drive drive, String currentPath, String newPath) throws IOException;

	void rename(Drive drive, String currentPath, String newPath) throws IOException;

	void delete(Drive drive, String path) throws IOException;
	
	void deleteFile(Drive drive, String path) throws IOException;
	
	void deleteDirectory(Drive drive, String path) throws IOException;

	void mkdir(Drive drive, String path) throws IOException;

	byte[] downloadBytes(Drive drive, String path, long startByte, int numberOfBytes) throws IOException;

	String uploadPartStart(Drive drive, String path) throws IOException;

	default void downloadPartStart(Drive drive, String path) throws IOException {

	}

	default void downloadComplete(Drive drive, String path) throws IOException {

	}

	void uploadPart(Drive drive, String path, byte[] data, int partNumber) throws IOException;
	
	void uploadPartComplete(Drive drive, String path, String uploadId) throws IOException;

	void uploadPartAbort(Drive drive, String path, String uploadId) throws IOException;
	
	/**
	 * Determines whether a file or directory already exist
	 * @param drive the drive to check
	 * @param path the path to check
	 * @return true if the provider can determine if this
	 *  file or directory exists; false otherwise
	 * @throws IOException when a problem occurs that prevents the
	 *     provider from giving an accurate answer
	 */
	boolean exists(Drive drive, String path) throws IOException;

	/**
	 * @param path the location of a file or directory
	 * @return true if the path represents a directory; false otherwise
	 */
	boolean isDirectory(Drive drive, String path) throws IOException;
	
	/** Create a path that is acceptable to the provider.
	 * (This is for fixing Franken-paths created when constructing
	 * destination paths by appending paths from two different providers.)
	 * @param path represents a path to a file or directory
	 * @return
	 */
	String normalizePath(String path);
	
	void uploadDirectory(Drive drive, String folderDrivePath, File directory) throws IOException;

	/**
	 * finds ONE DriveItem at the specified path
	 * @param path absolute path to the file
	 */
	DriveItem getDriveItem(Drive drive, String path) throws IOException;

	List<DriveItem> findAllInPath(Drive drive, String path) throws IOException;

	/**
	 * Returns the sourcePath's location in a format for a Hive external table
	 * @param sourceDrive 
	 * @param sourcePath the location of the data file
	 */
	String getHiveLocationPath(Drive sourceDrive, String sourcePath) throws IOException;

	/**
	 * Get an input stream for the object.
	 * @param drive the drive with the object
	 * @param sPathIn the path to the object
	 * @return an input stream from which the objects
	 *  contents can be retrieved.
	 * @throws IOException
	 */
	// Developer note: Getting an input stream may
	// require opening multiple resources, depending on the provider.
	// Closing those resources appropriately will require additional work.
	InputStream getInputStream(Drive drive, String sPathIn) throws IOException;

	/**
	 * @return a list of keys of the DriveProperties for a given provider
	 */
	List<String> getProperties();

    /**
     * Gather disk usage data for all of the files on the drive
     * and save it to the database.
     * @param start when data collection started
     */
    long findAndSaveFileMemoryUsage(Drive drive, EntityManager mgr, Date start) throws ExplorerException;
}
