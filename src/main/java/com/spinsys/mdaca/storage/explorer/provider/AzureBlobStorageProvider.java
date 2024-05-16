package com.spinsys.mdaca.storage.explorer.provider;

import static com.spinsys.mdaca.storage.explorer.io.FileUtil.PLACEHOLDER_FILE_NAME;
import static com.spinsys.mdaca.storage.explorer.io.PathProcessor.addLastSlash;
import static com.spinsys.mdaca.storage.explorer.io.PathProcessor.convertToUnixStylePath;
import static com.spinsys.mdaca.storage.explorer.io.PathProcessor.getFileName;
import static com.spinsys.mdaca.storage.explorer.io.PathProcessor.getParentFolderPath;
import static com.spinsys.mdaca.storage.explorer.io.PathProcessor.isRoot;
import static com.spinsys.mdaca.storage.explorer.io.PathProcessor.matchesPath;
import static com.spinsys.mdaca.storage.explorer.io.PathProcessor.removeFirstSlash;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import javax.persistence.EntityManager;

import org.apache.commons.collections4.IteratorUtils;
import org.apache.commons.lang3.StringUtils;

import com.azure.storage.blob.models.BlockBlobItem;
import com.azure.storage.blob.specialized.BlockBlobClient;
import com.azure.storage.blob.specialized.SpecializedBlobClientBuilder;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobProperties;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlobDirectory;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.CloudPageBlob;
import com.microsoft.azure.storage.blob.ListBlobItem;
import com.microsoft.azure.storage.blob.StandardBlobTier;
import com.spinsys.mdaca.storage.explorer.io.BasicFile;
import com.spinsys.mdaca.storage.explorer.io.FileUtil;
import com.spinsys.mdaca.storage.explorer.model.DriveItem;
import com.spinsys.mdaca.storage.explorer.model.DriveQuery;
import com.spinsys.mdaca.storage.explorer.model.StorageClass;
import com.spinsys.mdaca.storage.explorer.model.exception.ExplorerException;
import com.spinsys.mdaca.storage.explorer.persistence.Drive;
import com.spinsys.mdaca.storage.explorer.persistence.UsageDataCacher;

public class AzureBlobStorageProvider extends RestorableCloudStorageProvider<ListBlobItem> {

    public static final String HOT = "HOT";
    public static final String COOL = "COOL";
    public static final String ARCHIVE = "ARCHIVE";

    private static final Logger logger =
            Logger.getLogger("com.spinsys.mdaca.storage.explorer.provider.AzureBlobStorageProvider");

    /** Azure Blob details */
    public static final String BLOB_CONNECTION_STRING_PROPERTY_KEY = "BlobConnectionString";
    public static final String BLOB_CONTAINER_NAME_PROPERTY_KEY = "BlobContainerName";

    /** The client for uploading chunks */
    BlockBlobClient blockBlobClient = null;

    /** The list of chunks being uploaded */
    ArrayList<String> chunkIds = new ArrayList<>();

    @Override
    public List<DriveItem> find(Drive drive, DriveQuery query) throws ExplorerException {
        CloudBlobContainer container = getContainer(drive);

        Iterable<ListBlobItem> listBlobItems = getListBlobItems(query, container);
        List<ListBlobItem> blobItems = IteratorUtils.toList(listBlobItems.iterator());

        return buildAndFilterDriveItems(blobItems, drive, query);
    }

    @Override
    public long findAndSaveFileMemoryUsage(Drive drive,
            EntityManager entityManager, Date date) throws ExplorerException {
        long totalBytes = 0L;
        int driveId = drive.getDriveId();
        
        DriveQuery query = new DriveQuery();
        query.setRecursive(true);
        query.setSearchPattern(".*");
        query.setStartPath("");
        query.setUsesPlaceholder(true);
        query.setDriveId(driveId);

        CloudBlobContainer container = getContainer(drive);
        Object[] metadata = getMetadataForBuildDriveItem(drive);
        int batchSize = 1024;
        ArrayList<DriveItem> itemBatch = new ArrayList<>(batchSize);
        long count = 0L;

        Iterator<ListBlobItem> iterator =
                getListBlobItems(query, container).iterator();

        while (iterator.hasNext()) {
            ListBlobItem blobItem = iterator.next();
            DriveItem item = buildDriveItem(blobItem, drive, metadata);
            itemBatch.add(item);
            totalBytes += item.getFileSize();
            
            // Periodically save and clear out the blob list to conserve memory
            if ((++count % batchSize) == 0) {
                UsageDataCacher.saveFileUsageDataAndClear(entityManager, drive, itemBatch, date, false);
                
                if ((count % (50 * batchSize)) == 0) {
                    logger.info("Saved " + count + " items for " + drive);
                }
                itemBatch.clear();
            }
            
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
        UsageDataCacher.saveFileUsageDataAndClear(entityManager, drive, itemBatch, date, true);
        logger.info("Saved a total of " + count + " items for " + drive);

        return totalBytes;
    }

    private Iterable<ListBlobItem> getListBlobItems(DriveQuery query, CloudBlobContainer container) {
        //need to include the search past as the first parameter in listBlobs;
        // this allows us to also include query.isRecursive()
        String startPath = query.hasStartPath() ? query.getStartPath() : "";

        return container.listBlobs(startPath, query.isRecursive());
    }

    @Override
    public BasicFile download(Drive drive, String path) throws IOException {
        CloudBlobContainer container = getContainer(drive);

        try {
            CloudBlob blob = container.getBlobReferenceFromServer(path);

            File tempFile = FileUtil.buildTempFile("Az_download");
            String tempPath = tempFile.getCanonicalPath();
            blob.downloadToFile(tempPath);
            return new BasicFile(tempPath);
        } catch (URISyntaxException | StorageException e) {
            throw new IOException("Unable to download file from Azure blob because: " + e.getMessage(), e);
        }
    }

    //TODO implement upload directory
    @Override
    public void upload(Drive drive, String path, File file) throws IOException {
        CloudBlobContainer container = getContainer(drive);

        removePlaceholderFileIfPresent(drive, getParentFolderPath(path));

        try {
            path = removeFirstSlash(path);

            CloudBlockBlob blob = container.getBlockBlobReference(path);

            if (!blob.exists()) {
                blob.uploadFromFile(file.getAbsolutePath());
            } else {
                throw new IOException("The following file already exists: " + file.getName());
            }
        } catch (StorageException | URISyntaxException e) {
            throw new IOException("Unable to upload file to Azure Blob because: " + e.getMessage(), e);
        }
    }

    private void removePlaceholderFileIfPresent(Drive drive, String path) throws IOException {
        String placeholderFilePath = addLastSlash(path) + PLACEHOLDER_FILE_NAME;
        if (exists(drive, placeholderFilePath)) {
            deleteFile(drive, placeholderFilePath);
        }
    }

    @Override
    public void delete(Drive drive, String path) throws IOException {
        super.delete(drive, path);

        uploadPlaceholderFileIfApplicable(drive, getParentFolderPath(path));
    }

    @Override
    public void rename(Drive drive, String currentPath, String newPath) throws IOException {
        copy(drive, currentPath, newPath);
        delete(drive, currentPath);
    }

    @Override
    public void copy(Drive drive, String currentPathIn, String newPathIn) throws IOException {
        String currentPath = removeFirstSlash(currentPathIn);
        String newPath = removeFirstSlash(newPathIn);
        if (isDirectory(drive, currentPath )) {
            copyDirectory(drive, currentPath, newPath);
        }
        else {
            copyOneObject(drive, currentPath, newPath);
        }
    }

    void copyDirectory(Drive drive, String currentPath, String newPath) throws IOException {
        DriveQuery query = new DriveQuery();
        query.setDriveId(drive.getDriveId());
        query.setStartPath(currentPath);
        query.setRecursive(true);
        query.setUsesPlaceholder(true);

        List<DriveItem> driveItems = find(drive, query)
                .stream()
                .filter(driveItem -> !driveItem.isDirectory())
                //.map(DriveItem::getPath)
                .sorted(Comparator.comparing(DriveItem::getPath))
                .collect(Collectors.toList());

        //if the size is 0, then only the placeholder file needs to be uploaded
        if (driveItems.size() == 0) {
            mkdir(drive, newPath);
            return;
        }

        // Copy the objects in the directory one at a time.
        // An exception may be thrown in the middle of the for loop,
        // in which case there will be a partial copy.  No cleanup
        // is attempted.
        for (DriveItem di : driveItems) {
            String oldKey = di.getPath();
            String oldTail = oldKey.substring(currentPath.length());
            String newKey = newPath + oldTail;
            copy(drive, oldKey, newKey);
        }
    }


    protected void copyOneObject(Drive drive, String currentPath, String newPath) throws IOException {
        CloudBlobContainer container = getContainer(drive);

        try {
            CloudBlob newBlob = container.getPageBlobReference(newPath);
            CloudPageBlob oldBlob = container.getPageBlobReference(currentPath);

            URI uri = oldBlob.getUri();
            newBlob.startCopy(uri);
            newBlob.getCopyState();

            //NOTE: get CopyState finishes immediately for small files
            //TODO consider SECONDS_TO_WAIT to be something more dynamic
            int SECONDS_TO_WAIT = 3;
            long startTime = System.currentTimeMillis();
            boolean success = false;

            while (!success) {
                switch (newBlob.getCopyState().getStatus()) {
                case PENDING:
                    long elapsedTime = System.currentTimeMillis() - startTime;
                    if (TimeUnit.MILLISECONDS.toSeconds(elapsedTime) < SECONDS_TO_WAIT) {
                        throw new IOException("Timed out waiting for copy to occur.");
                    }
                    break;
                case UNSPECIFIED:
                case INVALID:
                case ABORTED:
                case FAILED:
                    throw new IOException("Was unable to copy the blob file successfully");
                case SUCCESS:
                    success = true;
                    break;
                }
            }
        } catch (URISyntaxException | StorageException e) {
            throw new IOException("Unable to rename file in Azure because " + e.getMessage(), e);
        }

        if (!getFileName(currentPath).equals(PLACEHOLDER_FILE_NAME)) {
            removePlaceholderFileIfPresent(drive, getParentFolderPath(currentPath));
            removePlaceholderFileIfPresent(drive, getParentFolderPath(newPath));
        }
    }

    @Override
    public void deleteFile(Drive drive, String path) throws IOException {
        path = removeFirstSlash(path);
        CloudBlobContainer container = getContainer(drive);

        try {
            CloudBlob blob = container.getBlobReferenceFromServer(path);

            blob.deleteIfExists();
        } catch (URISyntaxException | StorageException e) {
            throw new IOException("Unable to delete an Aure blob because: " + e.getMessage(), e);
        }
    }

    @Override
    public void deleteDirectory(Drive drive, String pathIn) throws IOException {
        String path = removeFirstSlash(pathIn);
        DriveQuery query = new DriveQuery(path);
        query.setRecursive(true);
        query.setUsesPlaceholder(true);
        List<DriveItem> driveItems = find(drive, query);

        for (DriveItem driveItem : driveItems) {
            if (driveItem.isFile()) {
                deleteFile(drive, driveItem.getPath());
            }
        }
    }

    /**
     * @param drive drive to create directory in
     * @param path  path of the directory to be created
     * @throws IOException
     * @see AzureBlobStorageProvider.PLACEHOLDER_FILE
     */
    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Override
    public void mkdir(Drive drive, String pathIn) throws IOException {
        String path = removeFirstSlash(addLastSlash(pathIn));
        try {
            uploadPlaceholderFileIfApplicable(drive, path);

            removePlaceholderFileIfPresent(drive, getParentFolderPath(path));
        } catch (Exception e) {
            throw new IOException("Unable to create directory because: " + e.getMessage(), e);
        }
    }

    private void uploadPlaceholderFileIfApplicable(Drive drive, String folderPath) throws IOException {
        String placeholderFilePath = addLastSlash(folderPath) + PLACEHOLDER_FILE_NAME;

        //add the placeholder file if the path is not the root AND no other files exist
        boolean hasFiles = find(drive, new DriveQuery(folderPath)).size() > 0;
        if (!isRoot(folderPath) && !hasFiles) {
            upload(drive, placeholderFilePath, FileUtil.buildPlaceholderFile());
        }
    }

    @Override
    public boolean exists(Drive drive, String path) throws IOException {
        path = removeFirstSlash(path);

        CloudBlobContainer container = getContainer(drive);

        try {
            CloudBlockBlob blockBlobReference = container.getBlockBlobReference(path);

            //if the file doesn't exist, check if the folder exists
            //will short circuit if blockBolbRef.exists() returns true
            return blockBlobReference.exists() || isDirectory(drive, path);
        } catch (URISyntaxException | StorageException e) {
            throw new IOException("Unable to test if path exists because: " + e.getMessage(), e);
        }
    }

    @Override
    public boolean isDirectory(Drive drive, String path) throws IOException {
        CloudBlobContainer container = getContainer(drive);

        try {
            CloudBlobDirectory blobDirectory = container.getDirectoryReference(path);

            //Azure directories always have at least ONE item inside them, if they exist
            return blobDirectory.listBlobs().iterator().hasNext();
        } catch (StorageException | URISyntaxException e) {
            throw new IOException("Unable to read path \"" + path + "\" because: \n" + e.getMessage(), e);
        }
    }

    @Override
    public String normalizePath(String path) {
        return convertToUnixStylePath(path);
    }

    @Override
    public void uploadDirectory(Drive drive, String folderPath, File directory) throws IOException {
        if (!directory.exists()) {
            throw new ExplorerException("Folder does not exist");
        }
        if (!directory.isDirectory()) {
            throw new ExplorerException("Expected to receive a folder, but instead got a file");
        }

        folderPath = (folderPath == null) ? "" : addLastSlash(folderPath);

        if (directory.listFiles().length > 0) {
            for (File file : directory.listFiles()) {
                String fileAbsolutePath = folderPath + file.getName();
                if (file.isFile()) {
                    upload(drive, fileAbsolutePath, file);
                } else {
                    uploadDirectory(drive, fileAbsolutePath, file);
                }
            }
        } else {
            //the empty folder will need a placeholder file to keep it from being deleted by Azure
            uploadPlaceholderFileIfApplicable(drive, folderPath);
        }
    }

    final HashMap<String, StorageClass> _storageClasses = new HashMap<String, StorageClass>() {{
        put(HOT, new StorageClass(HOT, "Hot", false, true));
        put(COOL, new StorageClass(COOL, "Cool", false));
        put(ARCHIVE, new StorageClass(ARCHIVE, "Archive", true));
    }};

    @Override
    public List<StorageClass> getStorageClasses() {
        return new ArrayList<>(_storageClasses.values());
    }

    @Override
    public void updateStorageClass(Drive drive, String path, StorageClass storageClass) throws IOException {
        CloudBlobContainer container = getContainer(drive);

        DriveQuery driveQuery = new DriveQuery();
        driveQuery.setSearchPattern(path);
        driveQuery.setStartPath(getParentFolderPath(path));

        Iterable<ListBlobItem> blobItemIterable = getListBlobItems(driveQuery, container);

        for (ListBlobItem listBlobItem : blobItemIterable) {
            String pathWithContainerName = listBlobItem.getUri().getPath();
            String containerName = container.getName();
            String simplePath = removeFirstSlash(pathWithContainerName.replace(containerName, ""));

            if (matchesPath(simplePath, path)) {
                updateOneStorageClass(storageClass, listBlobItem);
                break;
            }
        }
    }

    private void updateOneStorageClass(StorageClass storageClass, ListBlobItem blobItem) throws IOException {
        if (blobItem instanceof CloudBlockBlob) {
            CloudBlockBlob cloudBlockBlob = (CloudBlockBlob) blobItem;

            StandardBlobTier standardBlobTier = StandardBlobTier.valueOf(storageClass.getClassName());

            try {
                cloudBlockBlob.uploadStandardBlobTier(standardBlobTier);
            } catch (StorageException e) {
                throw new IOException("Unable to update storage class to \"" + storageClass + "\" for path: " +
                        blobItem.getUri().getPath());
            }
        }
    }

    @Override
    public boolean testConnection(Drive drive) throws ExplorerException {
        boolean exists = false;
        try {
            CloudBlobContainer container = getContainer(drive);
            exists = (container != null);
        } catch (Exception e) {
            throw new ExplorerException("Exception while testing connection to " +
                    drive + ". " + e.getMessage(), e);
        }
        return exists;
    }

    public CloudBlobContainer getContainer(Drive drive) throws ExplorerException {
        try {
            String sConnection = drive.getPropertyValue(BLOB_CONNECTION_STRING_PROPERTY_KEY);
            CloudStorageAccount storageAccount = CloudStorageAccount.parse(sConnection);
            CloudBlobClient blobClient = storageAccount.createCloudBlobClient();
            String containerName = drive.getPropertyValue(BLOB_CONTAINER_NAME_PROPERTY_KEY);
            return blobClient.getContainerReference(containerName);
        } catch (Exception e) {
            throw new ExplorerException("Unable to create Azure Blob connection because: " + e.getMessage(), e);
        }
    }

    @Override
    public DriveItem buildDriveItem(ListBlobItem blobItem, Drive drive, Object...metadata) {
        String containerName = (String) metadata[0];

        DriveItem driveItem = new DriveItem();

        String containerPath = "/" + containerName + "/";
        String path = blobItem.getUri().getPath()
                .replace(containerPath, "");

        driveItem.setDriveId(drive.getDriveId());
        driveItem.setPath(path);

        if (blobItem instanceof CloudBlockBlob) {
            CloudBlockBlob cloudBlockBlob = (CloudBlockBlob) blobItem;
            BlobProperties properties = cloudBlockBlob.getProperties();

            Date lastModified = properties.getLastModified();
            long length = properties.getLength();
            StandardBlobTier standardBlobTier = properties.getStandardBlobTier();

            if (standardBlobTier != null) {
                StorageClass storageClass = _storageClasses.get(standardBlobTier.name());
                driveItem.setStorageClass(storageClass);
            }
            boolean restoring = (properties.getRehydrationStatus() != null) &&
                    "PENDING_TO_HOT".equals(properties.getRehydrationStatus().name());

            driveItem.setModifiedDate(lastModified);
            driveItem.setFileSize(length);
            driveItem.setRestoring(restoring);
        } else if (blobItem instanceof CloudBlobDirectory) {
            driveItem.setDirectory(true);
        } else {
            //TODO possibly handle this more cleanly
            throw new RuntimeException("Unable to handle blob item type: " + blobItem.getClass());
        }

        return driveItem;
    }

    @Override
    protected Object[] getMetadataForBuildDriveItem(Drive drive) throws ExplorerException {
        CloudBlobContainer container = getContainer(drive);

        String name = container.getName();
        return new Object[]{name};
    }

    @Override
    public void restore(Drive drive, String path, int daysExpiration) throws IOException {
        StorageClass storageClass = _storageClasses.values().stream()
                .filter(StorageClass::isDefault)
                .findFirst().get();

        updateStorageClass(drive, path, storageClass);
    }

    @Override
    public boolean requiresDaysToExpire() {
        return false;
    }

    @Override
    public byte[] downloadBytes(Drive drive, String path, long startByte, int numBytes) throws IOException {
        CloudBlobContainer container = getContainer(drive);
        byte[] bytes = new byte[0];

        try {
            long start = System.currentTimeMillis();
            CloudBlockBlob blob = container.getBlockBlobReference(path);
            blob.downloadAttributes();
            long length = blob.getProperties().getLength();

            if (length > 0) {
                bytes = new byte[numBytes];
                int numRead = blob.downloadRangeToByteArray(startByte, (long)numBytes, bytes, 0);

                if (numRead < numBytes) { // only return the bytes actually read
                    if (numRead < 0) {
                        bytes = new byte[0];
                    }
                    else {
                        bytes = Arrays.copyOfRange(bytes, 0, numRead);
                    }
                    logger.info("Download of " + numRead + " bytes starting at " +
                            startByte + " from " + path + "took " +
                            (System.currentTimeMillis() - start) + " ms.");
                }
            }
        } catch (URISyntaxException | StorageException e) {
            throw new IOException("Unable to download bytes from Azure blob at " +
                    path + " because: " + e.getMessage(), e);
        }
        return bytes;
    }

    @Override
    public String uploadPartStart(Drive drive, String path) throws IOException {
        String sConnection = drive.getPropertyValue(BLOB_CONNECTION_STRING_PROPERTY_KEY);
        String containerName = drive.getPropertyValue(BLOB_CONTAINER_NAME_PROPERTY_KEY);
        blockBlobClient = new SpecializedBlobClientBuilder()
                .connectionString(sConnection)
                .containerName(containerName)
                .blobName(path)
                .buildBlockBlobClient();
        return blockBlobClient.getBlobName(); // or getBlobUrl?
    }

    @Override
    public void uploadPart(Drive drive, String path, byte[] data, int partNumber) throws IOException {
        if (data.length > 0) {
            String sPartNumber = StringUtils.leftPad("" + partNumber, 5, "0");
            String id = Base64.getEncoder().encodeToString(sPartNumber.getBytes());
            logger.info("Uploading part #" + partNumber + " with id " + id);

            long start = System.currentTimeMillis();
            InputStream chunkStream =
                    new BufferedInputStream(new ByteArrayInputStream(data));
            //			BlobOutputStream chunkStream = blockBlobClient.getBlobOutputStream();
            blockBlobClient.stageBlock(id, chunkStream, data.length);
            logger.info("Upload of " + data.length + " bytes took " +
                    (System.currentTimeMillis() - start ) + " ms.");
            chunkIds.add(id);
        }
    }

    @Override
    public void uploadPartComplete(Drive drive, String path, String uploadId) throws IOException {
        BlockBlobItem blockBlobItem =
                blockBlobClient.commitBlockList(chunkIds);
        logger.info("Upload to " + path + " completed");
        String parentFolderPath = getParentFolderPath(path);
        removePlaceholderFileIfPresent(drive, parentFolderPath);

        // reset
        blockBlobClient = null;
        chunkIds = new ArrayList<>();
    }

    @Override
    public void uploadPartAbort(Drive drive, String path, String blobName) throws IOException {
        logger.info("Upload to " + path + " aborted");

        //		BlockList block = blockBlobClient.listBlocksWithResponse(BlockListType.ALL, leaseId, timeout, context).getValue();
        //		BlockListType BlockListType.UNCOMMITTED;
        //		BlockList blocks = blockBlobClient.listBlocks(BlockListType.UNCOMMITTED);
        //		blocks.getUncommittedBlocks().forEach(b -> b.);

        // reset
        blockBlobClient = null;
        chunkIds = new ArrayList<>();
    }

    @Override
    public String getHiveLocationPath(Drive drive, String sourcePath) throws IOException {
        String parentFolder = getParentFolderPath(sourcePath);
        String sConnection = drive.getPropertyValue(BLOB_CONNECTION_STRING_PROPERTY_KEY);
        try {
            CloudStorageAccount storageAccount = CloudStorageAccount.parse(sConnection);
            String container = drive.getPropertyValue(BLOB_CONTAINER_NAME_PROPERTY_KEY);
            String host = storageAccount.getBlobEndpoint().getHost();
            String uri = "wasbs://" + container + "@" + host + "/" + parentFolder;
            return uri;

        } catch (Exception e) {

            throw new IOException("Unable to getHiveLocationPath " +
                    sourcePath + " because: " + e.getMessage(), e);
        }

        //return "wasbs://file-transfer-share@spinfiletransfer.blob.core.windows.net/" + parentFolder;
    }

    /**
     * Get the topmost lines of a file
     * @param numLines how many lines to get from the top of the file
     * @param tempFile the file storing the topmost lines
     * @return the number of bytes read
     * @throws ExplorerException
     * @throws IOException 
     */
    protected BasicFile getTopLines(Drive drive, String path, int numLines) throws IOException {
        CloudBlobContainer container = getContainer(drive);
        CloudBlockBlob blob;
        try {
            blob = container.getBlockBlobReference(path);
        } catch (Exception e) {
            String message = e.getMessage();
            logger.log(Level.WARNING, message, e);
            throw new ExplorerException(message, e);
        }
        DriveItem driveItem = buildDriveItem(blob, drive, container.getName());
        BasicFile tempFile = getTopLinesFromDriveItem(drive, path, numLines, driveItem);
        return tempFile;
    }

    public InputStream getInputStream(Drive drive, String path) throws IOException {
        CloudBlobContainer container = getContainer(drive);
        InputStream input = null;

        try {
            path = removeFirstSlash(path);

            CloudBlockBlob blob = container.getBlockBlobReference(path);

            if (blob.exists()) {
                input = blob.openInputStream();
            } else {
                throw new IOException(path + " does not exist.");
            }
        } catch (StorageException | URISyntaxException e) {
            throw new IOException("Unable to get input stream because: " + e.getMessage(), e);
        }
        return input;
    }

    @Override
    public List<String> getProperties() {
        return Arrays.asList(
                BLOB_CONNECTION_STRING_PROPERTY_KEY,
                BLOB_CONTAINER_NAME_PROPERTY_KEY,
                HIVE_HOST_NAME,
                HIVE_PORT);
    }

}
