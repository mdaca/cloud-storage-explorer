package com.spinsys.mdaca.storage.explorer.provider;

import static com.spinsys.mdaca.storage.explorer.io.PathProcessor.WINDOWS_SEP;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.logging.Logger;

import javax.persistence.EntityManager;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.io.filefilter.RegexFileFilter;

import com.spinsys.mdaca.storage.explorer.io.BasicFile;
import com.spinsys.mdaca.storage.explorer.io.FileUtil;
import com.spinsys.mdaca.storage.explorer.io.PathProcessor;
import com.spinsys.mdaca.storage.explorer.model.DriveItem;
import com.spinsys.mdaca.storage.explorer.model.exception.ExplorerException;
import com.spinsys.mdaca.storage.explorer.persistence.Drive;
import com.spinsys.mdaca.storage.explorer.persistence.UsageDataCacher;

/**
 * This class manipulates Windows files and directories on a
 *  mounted file system using basic Java I/O.
 * @author Keith Cassell
 *
 */
public class WindowsStorageProvider extends BasicStorageProvider {

    private static final Logger logger =
            Logger.getLogger("com.spinsys.mdaca.storage.explorer.provider.WindowsStorageProvider");

	public WindowsStorageProvider() {
		sep = PathProcessor.WINDOWS_SEP;
	}

	@Override
	public BasicFile download(Drive drive, String sPathIn) throws IOException {
		BasicFile tempFile = null;
		String sPath = getPathWithDriveLetter(drive, sPathIn);

		if (sPath != null && exists(drive, sPath)) {

			// TODO special handling for directories?
			tempFile = FileUtil.buildTempFile("WSP_download");
			try (FileOutputStream fos = new FileOutputStream(tempFile)) {
				Path path = Paths.get(sPath);
				Files.copy(path, fos);
				logger.info("Download from " + sPath + " to " +
						tempFile.getCanonicalPath() + " succeeded");
			}
		}
		else {
			logger.info("Unable to locate file to download: " + sPath);
		}
		return tempFile;
	}

	public void copy(Drive drive, String sOldPathIn, String sNewPathIn) throws IOException {
		String sOldPath = getPathWithDriveLetter(drive, sOldPathIn);
		String sNewPath = getPathWithDriveLetter(drive, sNewPathIn);

		super.copy(drive, sOldPath, sNewPath);
	}

	@Override
	public void rename(Drive drive, String sOldPathIn, String sNewPathIn) throws IOException {
		String sOldPath = getPathWithDriveLetter(drive, sOldPathIn);
		String sNewPath = getPathWithDriveLetter(drive, sNewPathIn);

		super.rename(drive, sOldPath, sNewPath);
	}

	@Override
	public void deleteFile(Drive drive, String destPath) throws IOException {
		String sPath = getPathWithDriveLetter(drive, destPath);

		Path path = Paths.get(sPath);
		Files.delete(path);
	}

	@Override
	public void deleteDirectory(Drive drive, String sPathIn) throws IOException {
		String sPath = getPathWithDriveLetter(drive, sPathIn);

		if (sPath != null && isDirectory(drive, sPath)
				&& !(sPath.startsWith("C:") && sPath.length() < 4)
				// TODO remove temporary safety check preventing wiping out the C drive
			) {
			Path path = Paths.get(sPath);
			File dir = new File(path.toString());
			FileUtils.deleteDirectory(dir);
		}
	}

	@Override
	public boolean exists(Drive drive, String sPathIn) throws IOException {
		boolean exists = false;
		String sPath = getPathWithDriveLetter(drive, sPathIn);

		if (sPath != null) {
			Path path = Paths.get(sPath);
			exists = Files.exists(path);
		}
		return exists;
	}

	@Override
	public void uploadDirectory(Drive drive, String fullDirPath, File directory) throws ExplorerException, IOException {
		if (fullDirPath == null) {
			fullDirPath = "";
		}

		String directoryName = directory.getName();

		if (!directory.exists()) {
			throw new ExplorerException("Directory \"" + directoryName + "\" does not exist.");
		}
		if (!directory.isDirectory()) {
			throw new ExplorerException("File \"" + directoryName + "\" is not a directory");
		}

		mkdir(drive, fullDirPath);

		if (directory.listFiles().length > 0) {
			for (File file : directory.listFiles()) {
				String fileAbsolutePath =
						PathProcessor.addLastSlash(fullDirPath) + file.getName();
				if (file.isFile()) {
					upload(drive, fileAbsolutePath, file);
				} else {
					uploadDirectory(drive, PathProcessor.addLastSlash(fileAbsolutePath), file);
				}
			}
		}
	}

	/**
	 * Get the topmost lines of a file
	 * @param numLines how many lines to get from the top of the file
	 * @param tempFile the file storing the topmost lines
	 * @return the number of bytes read
	 * @throws ExplorerException
	 * @throws IOException 
	 */
    protected BasicFile getTopLines(Drive drive, String sPathIn, int numLines) throws IOException {
		String path = getPathWithDriveLetter(drive, sPathIn);
		Object[] metadata = getMetadataForBuildDriveItem(drive);
		DriveItem driveItem = buildDriveItem(new File(path), drive, metadata);
		BasicFile tempFile = getTopLinesFromDriveItem(drive, path, numLines, driveItem);
		return tempFile;
	}

    @Override
    public long findAndSaveFileMemoryUsage(Drive drive,
            EntityManager entityManager, Date startDate) throws ExplorerException {
        long totalBytes = 0L;
        String driveLetter = drive.getPropertyValue(DRIVE_NAME_PROPERTY_KEY);
        String startPath = driveLetter + ":" + WINDOWS_SEP;
        File start = new File(startPath);
        RegexFileFilter regexFilter = new RegexFileFilter(".*");
        Collection<File> files =
                FileUtils.listFilesAndDirs(start, regexFilter, DirectoryFileFilter.DIRECTORY);
        logger.info("Found " + files.size() + " on drive " + drive);
        logger.info("Finished collecting usage data for " + drive);

        totalBytes = saveDataUsingIter(drive, files, entityManager, startDate);
        return totalBytes;
    }
    

    long saveDataUsingIter(Drive drive, Collection<File> files,
            EntityManager entityManager, Date startDate)
    {
        long totalBytes = 0L;
        int driveId = drive.getDriveId();
        int batchSize = 1024;

        ArrayList<DriveItem> items = new ArrayList<>(batchSize);

        Iterator<File> iterator = files.iterator();
        long count = 0L;

        while (iterator.hasNext()) {
            File file = iterator.next();
            DriveItem item = createUsageDriveItem(file, driveId);
            items.add(item);
            totalBytes += item.getFileSize();
            
            // Periodically save and clear out the item list to conserve memory
            if ((++count % batchSize) == 0) {
                UsageDataCacher.saveFileUsageDataAndClear(entityManager, drive, items, startDate, false);
                
                if ((count % (50 * batchSize)) == 0) {
                    logger.info("Saved " + count + " items for " + drive);
                }
                items.clear();
            }
            // Shrink the file list to reclaim memory
            iterator.remove();

        	double percent = 0;
			try {
				percent = FileUtil.getProcessCpuLoad();
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}

            logger.info("cpu percent " + percent);
            //If cpu usage is greater then 50%
            if(percent > 2.5){
                 try {
                    logger.info("Sleeping 5 seconds");
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
            }
        }
        // Save any leftovers
        UsageDataCacher.saveFileUsageDataAndClear(entityManager, drive, items, startDate, true);
        logger.info("Saved a total of " + count + " items for " + drive);
        return totalBytes;
    }

    private DriveItem createUsageDriveItem(File file, int driveId) {
        DriveItem item = new DriveItem();
        item.setDriveId(driveId);
        item.setFileSize(file.length());
        Path path = file.toPath();
        String sPath = PathProcessor.convertToUnixStylePath(path.toString());
        sPath = PathProcessor.removeDriveLetter(sPath);
        item.setPath(sPath);
        return item;
    }


}
