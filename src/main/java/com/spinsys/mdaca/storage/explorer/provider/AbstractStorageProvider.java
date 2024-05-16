package com.spinsys.mdaca.storage.explorer.provider;

import static com.spinsys.mdaca.storage.explorer.io.FileUtil.buildTempFile;
import static com.spinsys.mdaca.storage.explorer.io.PathProcessor.addLastSlash;
import static com.spinsys.mdaca.storage.explorer.io.PathProcessor.getFileName;
import static com.spinsys.mdaca.storage.explorer.io.PathProcessor.getParentFolderPath;
import static com.spinsys.mdaca.storage.explorer.io.PathProcessor.isNewLine;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.persistence.EntityManager;

import org.apache.commons.io.FileUtils;

import com.spinsys.mdaca.storage.explorer.io.BasicFile;
import com.spinsys.mdaca.storage.explorer.io.PathProcessor;
import com.spinsys.mdaca.storage.explorer.model.DriveItem;
import com.spinsys.mdaca.storage.explorer.model.DriveQuery;
import com.spinsys.mdaca.storage.explorer.model.exception.ExplorerException;
import com.spinsys.mdaca.storage.explorer.persistence.Drive;
import com.spinsys.mdaca.storage.explorer.persistence.UsageDataCacher;

public abstract class AbstractStorageProvider<T> implements StorageProvider {

    private static final Logger logger =
            Logger.getLogger("com.spinsys.mdaca.storage.explorer.provider.AbstractStorageProvider");

    protected abstract DriveItem buildDriveItem(T storageItem, Drive drive, Object... metadata);

    /**
     * maps a list of storageItems to DriveItems
     */
    protected List<DriveItem> buildDriveItems(List<T> storageItems, Drive drive) throws ExplorerException {
        Object[] metadata = getMetadataForBuildDriveItem(drive);
        ArrayList<DriveItem> driveItems = new ArrayList<>();

        for (T storageItem : storageItems) {
            DriveItem driveItem = buildDriveItem(storageItem, drive, metadata);
            driveItems.add(driveItem);
        }
        return driveItems;
    }

    /**
     * maps a list of storageItems (based on the generic type)
     * and filters the mapped DriveItems by the DriveQuery
     */
    protected List<DriveItem> buildAndFilterDriveItems(List<T> storageItems, Drive drive, DriveQuery query) throws ExplorerException {
        Object[] metadata = getMetadataForBuildDriveItem(drive);
        ArrayList<DriveItem> driveItems = new ArrayList<>();

        for (T storageItem : storageItems) {
            DriveItem driveItem = buildDriveItem(storageItem, drive, metadata);
            if (query.isIncluded(driveItem)) {
                driveItems.add(driveItem);
            }
        }
        return driveItems;
    }

    private void addAncestors(DriveQuery query, DriveItem driveItem, Set<DriveItem> driveItems) {
        DriveItem parentItem = driveItem.getParentItem();

        //add directory item; the HashSet will avoid duplicates
        while (query.isIncluded(parentItem)) {
            driveItems.add(parentItem);
            //continue checking for supsequent parents
            parentItem = parentItem.getParentItem();
        }
    }

    protected Object[] getMetadataForBuildDriveItem(Drive drive) throws ExplorerException {
        return new Objects[]{};
    }

    @Override
    public DriveItem getDriveItem(Drive drive, String path) throws ExplorerException {
        String parentFolderPath = getParentFolderPath(path);
        String fileName = getFileName(path);
        List<DriveItem> itemList = find(drive, new DriveQuery(parentFolderPath));

        return itemList.stream()
                .filter(item -> item.getFileName().equals(fileName))
                .findFirst().get();
    }

    @Override
    public void delete(Drive drive, String destPath) throws IOException {
        if (isDirectory(drive, destPath)) {
            deleteDirectory(drive, destPath);
        } else {
            deleteFile(drive, destPath);
        }
    }

    @Override
    public void deleteDirectory(Drive drive, String path) throws IOException {
        DriveQuery query = new DriveQuery(path);
        query.setRecursive(true);
        query.setUsesPlaceholder(true);
        List<DriveItem> driveItems = find(drive, query);
        TreeSet<String> dirs = new TreeSet<>();

        // Delete all the files in the directories first
        for (DriveItem driveItem : driveItems) {
            String diPath = driveItem.getPath();

            if (driveItem.isFile()) {
                deleteFile(drive, diPath);
            } else { // collect the directories to delete
                dirs.add(diPath);
            }
        }

        // Delete the directories that no longer contain files.
        // Because they are in descending order, they will be
        // empty when we delete.
        for (String dir : dirs.descendingSet()) {
            deleteFile(drive, dir);
        }
    }

    @Override
    public void uploadDirectory(Drive drive, String fullDirPath, File directory) throws IOException {
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

        File[] files = directory.listFiles();
        if (files.length > 0) {

            for (File file : files) {
                String fileAbsolutePath =
                        addLastSlash(fullDirPath) + file.getName();
                if (file.isFile()) {
                    upload(drive, fileAbsolutePath, file);
                } else {
                    uploadDirectory(drive, addLastSlash(fileAbsolutePath), file);
                }
            }
        }
    }

    public List<DriveItem> findAllInPath(Drive drive, String path) throws ExplorerException {
        DriveQuery query = new DriveQuery();
        query.setRecursive(true);
        query.setStartPath(path);

        return find(drive, query);
    }

    /**
     * Gather disk usage data for all of the files on the drive
     * and save it to the database.
     */
    public long findAndSaveFileMemoryUsage(Drive drive,
            EntityManager entityManager, Date start) throws ExplorerException {
        // Developer note: this implementation is generic and inefficient.
        // It should be specialized for each provider.
        long totalBytes = 0L;
        
        DriveQuery query = new DriveQuery();
        query.setRecursive(true);
        query.setSearchPattern(".*");
        query.setUsesPlaceholder(false);
        query.setDriveId(drive.getDriveId());

        List<DriveItem> items = find(drive, query);
        
        logger.info("Found " + items.size() + " drive items in " + drive);
        totalBytes = UsageDataCacher.saveFileUsageDataInBatches(drive, items, entityManager, start);
        return totalBytes;
    }
    
    
    @Override
    public boolean testConnection(Drive drive) throws ExplorerException {
        boolean exists;

        try {
            DriveQuery query = new DriveQuery("");
            query.setRecursive(false);
            query.setSearchPattern(".*");
            List<DriveItem> items = find(drive, query);
            exists = items != null && items.size() > 0;
        } catch (Exception e) {
            throw new ExplorerException("Exception while testing connection to " +
                    drive + ". " + e.getMessage(), e);
        }
        return exists;
    }

    /**
     * Get the topmost lines of a file
     * @param numLines how many lines to get from the top of the file
     * @param tempFile the file storing the topmost lines
     * @return the number of bytes read
     * @throws ExplorerException
     * @throws InterruptedException 
     * @throws IOException 
     */
    protected BasicFile getTopLines(Drive drive, String path, int numLines) throws IOException {
        DriveItem driveItem = getDriveItem(drive, path);
        BasicFile tempFile = getTopLinesFromDriveItem(drive, path, numLines, driveItem);

        return tempFile;
    }

    protected BasicFile getTopLinesFromDriveItem(Drive drive, String path, int numLines, DriveItem driveItem)
            throws IOException {
        final int downloadPartSize = 5000;
        final long maxSearchBytes = 500_000L;
        int topLinesCount = 0;
        long byteIndex = 0;
        long fileSize = driveItem.getFileSize();
        long endByte = fileSize;  // - downloadPartSize;
        BasicFile tempFile = buildTempFile("ASP_getTopLinesFromDriveItem");

        downloadPartStart(drive, path);

        do {
            byte[] startBytes = null;
            int retryCount = 20;
            Exception exception = null;
            boolean successful = false;

            // Download one part, possibly requiring retries
            while (!successful && retryCount > 0) {
                try {
                    retryCount--;
                    startBytes = downloadBytes(drive, path, byteIndex, downloadPartSize);
                    successful = true;
                } catch (Exception e) {
                    exception = e;
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e1) {
                        // ignore and try again
                    }
                }
            }

            if (!successful) {
                String message = "Unable to download bytes from " + path +
                        ": " +exception.getMessage();
                logger.log(Level.WARNING, message, exception);
                throw new ExplorerException(message, exception);
            }

            int index = 0;

            // Transfer the bytes from the part just downloaded
            while (topLinesCount < numLines && index < startBytes.length) {
                byte b = startBytes[index++];

                if (isNewLine(b)) {
                    topLinesCount++;

                    if (topLinesCount >= numLines) {
                        break;
                    }
                    // If it's a <CR><NL> combination, just treat it as a
                    // single end-of-line
                    if ((index < startBytes.length) &&
                            b == PathProcessor.CARRIAGE_RETURN &&
                            (startBytes[index] == PathProcessor.NEW_LINE)) {
                        index++;
                    }
                }
            }

            byte[] bytes = Arrays.copyOfRange(startBytes, 0, index);
            FileUtils.writeByteArrayToFile(tempFile, bytes, true);
            byteIndex += index;

            if (byteIndex > maxSearchBytes) {
                throw new ExplorerException("Search took too long to find the first \"" + numLines + "\" lines");
            }
            logger.info("topLinesCount = " + topLinesCount +
                    ", byteIndex = " + byteIndex);
        } while ((topLinesCount < numLines) && (byteIndex < endByte));

        downloadComplete(drive, path);
        return tempFile;
    }


}
