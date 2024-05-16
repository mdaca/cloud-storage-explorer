package com.spinsys.mdaca.storage.explorer.provider;

import static com.spinsys.mdaca.storage.explorer.io.PathProcessor.addLastSlash;
import static com.spinsys.mdaca.storage.explorer.io.PathProcessor.getParentFolderPath;
import static com.spinsys.mdaca.storage.explorer.io.PathProcessor.removeFirstSlash;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.TreeMap;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import javax.persistence.EntityManager;

import org.apache.commons.collections4.IterableUtils;
import org.apache.commons.io.FileUtils;

import com.google.api.gax.paging.Page;
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ReadChannel;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.CopyWriter;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobGetOption;
import com.google.cloud.storage.StorageOptions;
import com.spinsys.mdaca.storage.explorer.io.BasicFile;
import com.spinsys.mdaca.storage.explorer.io.FileUtil;
import com.spinsys.mdaca.storage.explorer.io.PathProcessor;
import com.spinsys.mdaca.storage.explorer.model.DriveItem;
import com.spinsys.mdaca.storage.explorer.model.DriveQuery;
import com.spinsys.mdaca.storage.explorer.model.StorageClass;
import com.spinsys.mdaca.storage.explorer.model.exception.ExplorerException;
import com.spinsys.mdaca.storage.explorer.model.http.TransferSpec;
import com.spinsys.mdaca.storage.explorer.persistence.Drive;
import com.spinsys.mdaca.storage.explorer.persistence.UsageDataCacher;

/**
 * @see https://cloud.google.com/storage/docs/reference/libraries
 * @see https://cloud.google.com/storage/docs/samples/
 * @author Keith Cassell
 *
 */
public class GoogleCloudStorageProvider extends CloudStorageProvider<Blob> {

    //Storage classes - https://cloud.google.com/storage/docs/storage-classes

    /** Standard Storage is best for data that is frequently accessed
     *  and/or stored for only brief periods of time. */
    public static final String STANDARD = com.google.cloud.storage.StorageClass.STANDARD.toString();

    /** Archive Storage is the best choice for data that you plan
     *  to access less than once a year. */
    public static final String ARCHIVE = com.google.cloud.storage.StorageClass.ARCHIVE.toString();

    /** For data you plan to read or modify at most once a quarter */
    public static final String COLD_LINE = com.google.cloud.storage.StorageClass.COLDLINE.toString();

    /** Nearline Storage is ideal for data you plan to read or modify
     * on average once per month or less. For data accessed less frequently
     * than once a quarter, Coldline Storage or Archive Storage
     * are more cost-effective, as they offer lower storage costs. */
    public static final String NEARLINE = com.google.cloud.storage.StorageClass.NEARLINE.toString();

    final TreeMap<String, StorageClass> _storageClasses = new TreeMap<String, StorageClass>() {
        private static final long serialVersionUID = 1L;
        {
            put(ARCHIVE, new StorageClass(ARCHIVE, "Archive", false));
            put(COLD_LINE, new StorageClass(COLD_LINE, "Cold Line", false));
            put(NEARLINE, new StorageClass(NEARLINE, "Near Line", false));
            put(STANDARD, new StorageClass(STANDARD, "Standard", false, true));
        }
    };

    /** Provider Properties */
    public static final String GOOGLE_BUCKET_NAME_PROPERTY_KEY = "GoogleBucketName";
    public static final String PROJECT_PROPERTY_KEY = "ProjectId";
    /** The key for where to find the JSON credentials for Google Cloud Storage */
    public static final String GOOGLE_CREDENTIALS = "GCSCredentials";

    private static final Logger logger =
            Logger.getLogger("com.spinsys.mdaca.storage.explorer.provider.GoogleCloudStorageProvider");

    Credentials credentials = null;

    Storage getStorage(Drive drive) throws ExplorerException {
        String projectId = drive.getPropertyValue(PROJECT_PROPERTY_KEY);
        String credentialsJSON = drive.getPropertyValue(GOOGLE_CREDENTIALS);
        Credentials credentials = getCredentials(credentialsJSON);
        Storage storage =
                StorageOptions.newBuilder().setCredentials(credentials)
                .setProjectId(projectId).build().getService();
        return storage;
    }

    Credentials getCredentials(String sCredentials) throws ExplorerException {
        if (credentials == null) {
            try (InputStream is = new ByteArrayInputStream(sCredentials.getBytes(StandardCharsets.UTF_8))) {
                //			FileInputStream is = new FileInputStream(credentialsFile);
                credentials = GoogleCredentials.fromStream(is);
            } catch (IOException e) {
                throw new ExplorerException(e);
            }
        }
        return credentials;
    }

    @Override
    public DriveItem buildDriveItem(Blob blob, Drive drive, Object...ignored) {
        DriveItem item = new DriveItem();
        String path = blob.getName();

        //		boolean isDir = isDirectory(drive, path);
        // Avoid the call to GCS to determine whether something is
        // a directory for display purposes
        boolean isDir = path.endsWith(PathProcessor.UNIX_SEP);
        item.setDirectory(isDir);
        Long updateTime = null;

        // "Directories" don't have the updateTime set when doing a "list()"
        if (isDir) {
            if (blob != null) {
                //todo check if this returns same value
                updateTime = blob.getUpdateTime();
            }
        } else {
            updateTime = blob.getUpdateTime();
        }

        if (updateTime != null) {
            item.setModifiedDate(new java.util.Date(updateTime));
        }
        item.setPath(path);
        item.setFileSize(blob.getSize());
        item.setDriveId(drive.getDriveId());
        com.google.cloud.storage.StorageClass blobStorageClass =
                blob.getStorageClass();

        if (blobStorageClass != null) {
            StorageClass storageClass = _storageClasses.get(blobStorageClass.toString());
            item.setStorageClass(storageClass);
        }
        return item;
    }

    @Override
    public DriveItem getDriveItem(Drive drive, String path) throws ExplorerException {
        try {
            Storage storage = getStorage(drive);
            String bucket = drive.getPropertyValue(GOOGLE_BUCKET_NAME_PROPERTY_KEY);

            BlobId blobId = BlobId.of(bucket, path);
            Blob blob = storage.get(blobId);

            return buildDriveItem(blob, drive, storage);
        } catch (IOException e) {
            throw new ExplorerException(e);
        }
    }

    @Override
    public BasicFile download(Drive drive, String objectName) throws IOException {
        if (objectName == null || !exists(drive, objectName)) {
            throw new FileNotFoundException("No file found at path - " + objectName);
        }
        BasicFile file = FileUtil.buildTempFile("GCS_download");
        String destFilePath = file.getAbsolutePath();
        String bucket = drive.getPropertyValue(GOOGLE_BUCKET_NAME_PROPERTY_KEY);
        Storage storage = getStorage(drive);

        BlobId blobId = BlobId.of(bucket, objectName);
        Blob blob = storage.get(blobId);
        blob.downloadTo(Paths.get(destFilePath));

        logger.info("Downloaded object " + objectName +
                " from bucket name " + bucket + " to " + destFilePath);
        return file;
    }

    @Override
    public void upload(Drive drive, String path, File file) throws IOException {
        if (file == null) {
            throw new FileNotFoundException("No file to upload specified.");
        }
        if (path == null) {
            throw new FileNotFoundException("No path specified.");
        }

        path = removeFirstSlash(path);
        if (exists(drive, path)) {
            throw new FileAlreadyExistsException("Attempted to upload \"" + path + "\", but it already exists");
        }

        Storage storage = getStorage(drive);
        String bucket = drive.getPropertyValue(GOOGLE_BUCKET_NAME_PROPERTY_KEY);
        BlobId blobId = BlobId.of(bucket, path);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
        String absolutePath = file.getAbsolutePath();
        uploadToStorage(storage, file, blobInfo);
        logger.info(absolutePath + " uploaded to bucket " + bucket + " as " + path);
    }

    // https://stackoverflow.com/questions/53628684/how-to-upload-a-large-file-into-gcp-cloud-storage
    private void uploadToStorage(Storage storage, File uploadFrom, BlobInfo blobInfo) throws IOException {
        // For small files:
        if (uploadFrom.length() < FileUtils.ONE_MB) {
            byte[] bytes = Files.readAllBytes(uploadFrom.toPath());
            storage.create(blobInfo, bytes);
        } else {
            // For big files:
            // When content is not available or large (1MB or more)
            // write it in chunks via the blob's channel writer.
            // TODO investigate parallel composite uploads
            // https://cloud.google.com/storage/docs/uploads-downloads
            try (WriteChannel writer = storage.writer(blobInfo)) {
                byte[] buffer = new byte[10240];
                try (InputStream input = Files.newInputStream(uploadFrom.toPath())) {
                    int limit;
                    while ((limit = input.read(buffer)) >= 0) {
                        writer.write(ByteBuffer.wrap(buffer, 0, limit));
                    }
                }
            }
        }
    }

    public void copy(Drive drive, TransferSpec spec) throws IOException {
        String sourcePath = spec.getSourcePath();
        if (sourcePath == null || !exists(drive, sourcePath)) {
            throw new FileNotFoundException("No file found at path - " + sourcePath);
        }
        String destPath = spec.getDestPath();
        if (destPath == null) {
            throw new FileNotFoundException("No destination path specified.");
        }
        if (exists(drive, destPath)) {
            throw new FileAlreadyExistsException("Attempted to copy to \"" + destPath + "\", but it already exists");
        }

        copy(drive, sourcePath, destPath);
    }

    public void copy(Drive drive, String sourcePath, String destPath) throws IOException {
        Storage storage = getStorage(drive);
        String bucket = drive.getPropertyValue(GOOGLE_BUCKET_NAME_PROPERTY_KEY);
        Blob blob = storage.get(bucket, sourcePath);
        // Write a copy of the object to the target bucket
        CopyWriter copyWriter = blob.copyTo(bucket, destPath);

        StorageClass storageClass = _storageClasses.get(blob.getStorageClass().toString());

        if (!storageClass.isDefault()) {
            updateStorageClass(drive, destPath, storageClass);
        }
        // TODO examine copyWriter
        // TODO use Bucket.BlobTargetOption.doesNotExist() ?
        logger.info("Copied object " + sourcePath + " to " + destPath +
                " with result - " + copyWriter);
    }

    @Override
    public void rename(Drive drive, String currentPath, String newPath) throws IOException {
        if (currentPath == null ||
                // a "directory" may be a prefix and not be an actual object
                (!exists(drive, currentPath) && !isDirectory(drive, currentPath))) {
            throw new FileNotFoundException("No file found at path - " + currentPath);
        }
        if (newPath == null) {
            throw new FileNotFoundException("No new path name specified.");
        }
        if (exists(drive, newPath)) {
            throw new FileAlreadyExistsException("Attempted to rename to \"" + newPath + "\", but it already exists");
        }
        /* Existing objects cannot be directly renamed. Instead, copy an object, give
         * the copied version the desired name, and delete the original version of the
         * object. */
        // Because there are no true directories in S3, we must
        // rename everything with the currentPath prefix.
        if (isDirectory(drive, currentPath)) {
            DriveQuery query = new DriveQuery();
            query.setDriveId(drive.getDriveId());
            query.setStartPath(currentPath);
            query.setRecursive(true);

            List<DriveItem> driveItems = find(drive, query);
            List<DriveItem> keysSorted = driveItems.stream()
                    .filter(driveItem -> !driveItem.isDirectory())
                    //.map(DriveItem::getPath)
                    .sorted((driveItemA, driveItemB) -> driveItemA.getPath().compareTo(driveItemB.getPath()))
                    .collect(Collectors.toList());

            // Move the objects in the directory one at a time.
            // An exception may be thrown in the middle of the for loop,
            // in which case there will be a partial copy.  No cleanup
            // is attempted.
            for (DriveItem di : keysSorted) {
                String oldKey = di.getPath();
                String oldTail = oldKey.substring(currentPath.length());
                String newKey = newPath + oldTail;

                copy(drive, oldKey, newKey);
            }
            delete(drive, currentPath);
        }
        else {
            copy(drive, currentPath, newPath);
            delete(drive, currentPath);
            logger.info("Renamed " + currentPath + " to " + newPath);
        }
    }

    @Override
    public void deleteFile(Drive drive, String path) throws IOException {
        if (path == null) {
            throw new FileNotFoundException("No source path specified.");
        }
        String bucket = drive.getPropertyValue(GOOGLE_BUCKET_NAME_PROPERTY_KEY);
        Storage storage = getStorage(drive);
        boolean wasDeleted = storage.delete(bucket, path);
        logger.info("Deleted " + path + " - " + wasDeleted);
    }

    @Override
    public void deleteDirectory(Drive drive, String path) throws IOException {
        super.deleteDirectory(drive, path);

        //delete the folder object
        deleteFile(drive, path);
    }

    @Override
    public void mkdir(Drive drive, String inPath) throws IOException {
        if (inPath == null) {
            throw new FileNotFoundException("No path specified.");
        }
        if (exists(drive, inPath)) {
            throw new FileAlreadyExistsException("Attempted to upload \"" + inPath + "\", but it already exists");
        }
        Storage storage = getStorage(drive);
        String bucket = drive.getPropertyValue(GOOGLE_BUCKET_NAME_PROPERTY_KEY);
        createDirectoryObject(storage, bucket, inPath);
    }

    ReadChannel reader;

    @Override
    public void downloadPartStart(Drive drive, String path) throws IOException {
        Blob blob = getBlob(drive, path);
        reader = blob.reader();
    }

    @Override
    public void downloadComplete(Drive drive, String path) {
        if (reader != null) {
            reader.close();
        }
    }

    @Override
    public byte[] downloadBytes(Drive drive, String path, long startByte, int numberOfBytes) throws IOException {
        ByteBuffer byteBuf = ByteBuffer.allocate(numberOfBytes);
        long start = System.currentTimeMillis();
        //		InputStream inputStream = Channels.newInputStream(reader);
        //		inputStream.read(byteArray, numberOfBytes, numberOfBytes)
        reader.seek(startByte);
        reader.setChunkSize(numberOfBytes);
        int read = reader.read(byteBuf);
        boolean isEnd = read == -1;
        logger.info("Download of " + read + " bytes took " +
                (System.currentTimeMillis() - start) + " ms.");

        if (isEnd) {
            return new byte[0];
        } else if (read < numberOfBytes) {
            return Arrays.copyOfRange(byteBuf.array(), 0, read);
        } else {
            return byteBuf.array();
        }
    }

    private Blob getBlob(Drive drive, String path) throws IOException {
        Storage storage = getStorage(drive);
        String bucketName = drive.getPropertyValue(GOOGLE_BUCKET_NAME_PROPERTY_KEY);

        return storage.get(bucketName, path);
    }

    public String uploadPartStart(Drive drive, String path) throws IOException {
        Storage storage = getStorage(drive);
        String bucketName = drive.getPropertyValue(GOOGLE_BUCKET_NAME_PROPERTY_KEY);
        BlobId blobId = BlobId.of(bucketName, path);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();

        writer = storage.writer(blobInfo);
        return "someId";
    }

    WriteChannel writer;

    public void uploadToStorage(Drive drive, File uploadFrom, String path) throws IOException {
        Storage storage = getStorage(drive);
        String bucketName = drive.getPropertyValue(GOOGLE_BUCKET_NAME_PROPERTY_KEY);
        BlobId blobId = BlobId.of(bucketName, path);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();

        // For big files:
        // When content is not available or large (1MB or more) it is recommended to write it in chunks via the blob's channel writer.
        try (WriteChannel writer = storage.writer(blobInfo)) {

            byte[] buffer = new byte[10_240];
            try (InputStream input = Files.newInputStream(uploadFrom.toPath())) {
                int limit;
                while ((limit = input.read(buffer)) >= 0) {
                    writer.write(ByteBuffer.wrap(buffer, 0, limit));
                }
            }

        }
    }

    public void uploadPart(Drive drive, String path, byte[] data, int i) throws IOException {
        long start = System.currentTimeMillis();
        writer.write(ByteBuffer.wrap(data, 0, data.length));//TODO change data.length here
        logger.info("Upload of " + data.length + " bytes took " +
                (System.currentTimeMillis() - start ) + " ms.");
    }

    public void uploadPartComplete(Drive drive, String path, String uploadId) throws IOException {
        writer.close();
    }

    public void uploadPartAbort(Drive drive, String path, String uploadId) throws IOException {
        writer.close();
    }

    void createDirectoryObject(Storage storage, String bucket, String inPath) {
        // There are no true directories.  We mimic one by creating a
        // file/blob with a slash at the end of the name and no content.
        // GCS doesn't really have "/" at the root, so we remove
        // the initial slash
        String path = addLastSlash(inPath);
        path = removeFirstSlash(path);
        BlobId blobId = BlobId.of(bucket, path);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
        storage.create(blobInfo);
        logger.info("Created directory: " + path);
    }

    @Override
    public boolean exists(Drive drive, String path) throws IOException {
        path = removeFirstSlash(path);
        boolean exists = blobExists(drive, path) || isDirectory(drive, path);
        return exists;
    }

    boolean blobExists(Drive drive, String path) throws IOException, FileNotFoundException {
        boolean exists = false;
        if (path != null) {
            String bucket = drive.getPropertyValue(GOOGLE_BUCKET_NAME_PROPERTY_KEY);
            Storage storage = getStorage(drive);
            BlobId blobId = BlobId.of(bucket, path);

            if (blobId != null) {
                Blob blob = storage.get(blobId);
                exists = (blob != null && blob.exists());
            }
        }
        return exists;
    }

    @Override
    public boolean isDirectory(Drive drive, String path) {
        // GCS doesn't truly have directories, but mimics them
        // somewhat via slash handling in some tools.
        boolean isDir = false;
        if (path != null) {
            String modPath = removeFirstSlash(path);
            modPath = addLastSlash(modPath);
            String bucket = drive.getPropertyValue(GOOGLE_BUCKET_NAME_PROPERTY_KEY);
            BlobId blobId = BlobId.of(bucket, modPath);
            isDir = ((blobId != null) && isPrefix(drive, modPath));
        }
        return isDir;
    }


    /**
     * @return true when the provided path is a prefix under which
     * one or more objects/files exist, i.e., the prefix appears
     * to be a directory; false otherwise.
     */
    public boolean isPrefix(Drive drive, String path) {
        boolean isDir = false;
        if (path != null) {
            path = removeFirstSlash(path);
            String bucket = drive.getPropertyValue(GOOGLE_BUCKET_NAME_PROPERTY_KEY);
            try {
                Storage storage = getStorage(drive);
                Page<Blob> blobs =
                        storage.list(bucket, Storage.BlobListOption.prefix(path),
                                Storage.BlobListOption.pageSize(1));
                isDir = blobs.getValues().iterator().hasNext();
            } catch (IOException e) {
                isDir = false;
            }
        }
        return isDir;
    }


    @Override
    public String normalizePath(String path) {
        String result = null;

        if (path != null) {
            result = path.replace('\\', '/');
        }
        return result;
    }

    @Override
    public List<DriveItem> find(Drive drive, DriveQuery query) throws ExplorerException {
        String sourcePath = query.getStartPath();
        if (sourcePath == null) {
            sourcePath = ""; // "root"
        }
        else {
            // make sure the path looks like a directory
            sourcePath = addLastSlash(sourcePath);

            // GCS doesn't follow a convention of having a slash
            // representing the root directory in the path
            sourcePath = removeFirstSlash(sourcePath);
        }
        Storage storage = getStorage(drive);
        String bucketName = drive.getPropertyValue(GOOGLE_BUCKET_NAME_PROPERTY_KEY);
        Bucket bucket = storage.get(bucketName);

        Storage.BlobListOption prefix = Storage.BlobListOption.prefix(sourcePath);
        Storage.BlobListOption currentDirectoryOnly = Storage.BlobListOption.currentDirectory();
        Page<Blob> blobPage =
                query.isRecursive() ? bucket.list(prefix)
                        : bucket.list(prefix, currentDirectoryOnly);

        ArrayList<DriveItem> driveItems = new ArrayList<>();

        do {
            List<Blob> blobList = IterableUtils.toList(blobPage.iterateAll());

            List<DriveItem> builtItems = buildAndFilterDriveItems(blobList, drive, query);
            driveItems.addAll(builtItems);
            blobPage = blobPage.getNextPage();
        } while (blobPage != null);

        return driveItems;
    }

    @Override
    public long findAndSaveFileMemoryUsage(Drive drive,
            EntityManager entityManager, Date date) throws ExplorerException {
        long totalBytes = 0L;
        long count = 0L;
        int driveId = drive.getDriveId();
        Storage storage = getStorage(drive);
        String bucketName = drive.getPropertyValue(GOOGLE_BUCKET_NAME_PROPERTY_KEY);
        Bucket bucket = storage.get(bucketName);

        DriveQuery query = new DriveQuery();
        query.setRecursive(true);
        query.setSearchPattern(".*");
        query.setStartPath("");
        query.setUsesPlaceholder(true);
        query.setDriveId(driveId);

        Page<Blob> blobPage = bucket.list();

        // Only save a batch worth of drive items at a time,
        // reclaiming memory after each batch
        int batchSize = 1024;
        ArrayList<DriveItem> itemBatch = new ArrayList<>(batchSize);

        // Process every blob
        for (Blob blob : blobPage.iterateAll()) {
            DriveItem driveItem = buildDriveItem(blob, drive);
            itemBatch.add(driveItem);

            // Periodically save and clear out the blob list to conserve memory
            if ((++count % batchSize ) == 0) {
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
        totalBytes +=
                UsageDataCacher.saveFileUsageDataAndClear(entityManager, drive, itemBatch, date, true);
        logger.info("Saved " + count  + " items for " + drive);
        return totalBytes;
    }

    /**
     * Add up and returns the total amount of bytes
     *  being used by objects in the drive that match the query.
     * @param drive the drive to check
     * @param query the conditions to satisfy (currently just the
     * 	start path location)
     * @return total bytes used
     */
    private long findBytesUsed(Drive drive, DriveQuery query) throws IOException
    {
        long bytesUsed = 0L;

        String sourcePath = query.getStartPath();
        if (sourcePath == null) {
            sourcePath = ""; // "root"
        }
        else {
            // make sure the path looks like a directory
            sourcePath = addLastSlash(sourcePath);

            // GCS doesn't follow a convention of having a slash
            // representing the root directory in the path
            sourcePath = removeFirstSlash(sourcePath);
        }
        Storage storage = getStorage(drive);
        String bucketName = drive.getPropertyValue(GOOGLE_BUCKET_NAME_PROPERTY_KEY);
        Bucket bucket = storage.get(bucketName);

        Storage.BlobListOption prefix = Storage.BlobListOption.prefix(sourcePath);
        Page<Blob> blobPage = bucket.list(prefix);

        for (Blob blob : blobPage.iterateAll()) {
            Long size = blob.getSize();
            bytesUsed += ((size != null) ? size : 0L);
        }
        logger.info(bucketName + " total bytes used = " + bytesUsed);
        return bytesUsed;
    }

    @Override
    public List<StorageClass> getStorageClasses() {
        return new ArrayList<StorageClass>(_storageClasses.values());
    }

    @Override
    public void updateStorageClass(Drive drive, String objectName, StorageClass newStorageClass) throws IOException {
        if (objectName == null) {
            throw new FileNotFoundException("No source path specified.");
        }
        if (newStorageClass == null) {
            throw new IOException("No storage class specified.");
        }
        String bucketName = drive.getPropertyValue(GOOGLE_BUCKET_NAME_PROPERTY_KEY);
        Storage storage = getStorage(drive);
        BlobId blobId = BlobId.of(bucketName, objectName);

        // See the StorageClass documentation for valid storage classes:
        // https://googleapis.dev/java/google-cloud-clients/latest/com/google/cloud/storage/StorageClass.html
        com.google.cloud.storage.StorageClass gcsStorageClass =
                com.google.cloud.storage.StorageClass.valueOf(newStorageClass.getClassName());

        // You can't change an object's storage class directly,
        // the only way is to rewrite the object with the
        // desired storage class
        Storage.CopyRequest request =
                Storage.CopyRequest.newBuilder()
                .setSource(blobId)
                .setTarget(BlobInfo.newBuilder(blobId).setStorageClass(gcsStorageClass).build())
                .build();
        Blob updatedBlob = storage.copy(request).getResult();

        logger.info("Object " + objectName  + " in bucket " + bucketName
                + " had its storage class set to " +
                updatedBlob.getStorageClass().name());
    }

    @Override
    public String getHiveLocationPath(Drive drive, String sourcePath) {

        String parentFolder = getParentFolderPath(sourcePath);
        return "gs://" + drive.getPropertyValue(GOOGLE_BUCKET_NAME_PROPERTY_KEY) + "/" + parentFolder;

    }

    @Override
    public boolean testConnection(Drive drive) throws ExplorerException {
        boolean exists = false;
        Storage storage;
        try {
            storage = getStorage(drive);
            String bucketName = drive.getPropertyValue(GOOGLE_BUCKET_NAME_PROPERTY_KEY);
            Bucket bucket = storage.get(bucketName);
            exists = (bucket != null);
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
     * @throws IOException 
     */
    protected BasicFile getTopLines(Drive drive, String path, int numLines) throws IOException {
        BlobGetOption fields = Storage.BlobGetOption.fields(Storage.BlobField.values());
        String bucket = drive.getPropertyValue(GOOGLE_BUCKET_NAME_PROPERTY_KEY);
        Storage storage = getStorage(drive);
        Blob blob = storage.get(bucket, path, fields);
        DriveItem driveItem = buildDriveItem(blob, drive);

        BasicFile tempFile = getTopLinesFromDriveItem(drive, path, numLines, driveItem);
        return tempFile;
    }

    public InputStream getInputStream(Drive drive, String path) throws IOException {
        Storage storage = getStorage(drive);
        String bucket = drive.getPropertyValue(GOOGLE_BUCKET_NAME_PROPERTY_KEY);
        ReadChannel reader = storage.reader(bucket, path);
        return Channels.newInputStream(reader);
    }

    @Override
    public List<String> getProperties() {
        return Arrays.asList(
                GOOGLE_BUCKET_NAME_PROPERTY_KEY,
                PROJECT_PROPERTY_KEY,
                GOOGLE_CREDENTIALS,
                HIVE_HOST_NAME,
                HIVE_PORT);
    }

}
