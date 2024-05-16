package com.spinsys.mdaca.storage.explorer.provider;

import static com.spinsys.mdaca.storage.explorer.io.PathProcessor.UNIX_SEP;
import static com.spinsys.mdaca.storage.explorer.io.PathProcessor.getParentFolderPath;
import static com.spinsys.mdaca.storage.explorer.io.PathProcessor.isRoot;
import static com.spinsys.mdaca.storage.explorer.io.PathProcessor.removeFirstSlash;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileAlreadyExistsException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.TimeZone;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import javax.persistence.EntityManager;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.RestoreObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.Copy;
import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.MultipleFileUpload;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
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

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;

public class AWSS3StorageProvider extends RestorableCloudStorageProvider<S3ObjectSummary> {

    /** AWS S3 */
    public static final String REGION_PROPERTY_KEY = "Region";
    public static final String BUCKET_NAME_PROPERTY_KEY = "BucketName";
    public static final String ACCESS_KEY_PROPERTY_KEY = "AccessKey";
    public static final String ACCESS_SECRET_PROPERTY_KEY = "AccessSecret";

    private static final Logger logger =
            Logger.getLogger("com.spinsys.mdaca.storage.explorer.provider.AWSS3StorageProvider");

    /** The client for communicating with S3 using AWS SDK for Java 2.x */
    S3Client s3Client = null;
    AmazonS3 amazonS3 = null;
    HeadObjectRequest headObjectRequest;
    HeadObjectResponse headObjectResponse;

    CreateMultipartUploadRequest multipartUploadRequest = null;
    CreateMultipartUploadResponse response = null;

    /** The list of chunks already uploaded. */
    ArrayList<CompletedPart> completedParts = new ArrayList<>();
    
    @Override
    public boolean isDirectory(Drive drive, String path) {
        // If the name ends in a slash, assume it's a directory
        boolean isDir = isDirectorySyntactically(path) && exists(drive, path);

        if ((path != null) && !isDir && (drive != null)) {
            isDir = isDirectorySemantically(drive, path);
        }
        return isDir;
    }

    /**
     * Determine whether the provided path appears to be a
     * directory by checking whether there are additional objects
     * in S3 whose keys have the path as a prefix
     * @param drive
     * @param path
     * @return true if the provided path is a "directory"; false otherwise
     */
    boolean isDirectorySemantically(Drive drive, String path) {
        boolean isDir = false;
        String startPath = PathProcessor.addLastSlash(path);
        String bucket = drive.getPropertyValue(BUCKET_NAME_PROPERTY_KEY);
        AmazonS3 s3 = getAuth(drive);
        ListObjectsRequest listObjectsRequest =
                new ListObjectsRequest().withBucketName(bucket).withPrefix(startPath).withDelimiter("/");
        ObjectListing objectListing = s3.listObjects(listObjectsRequest);

        if (objectListing != null) {
            List<S3ObjectSummary> summaries = objectListing.getObjectSummaries();
            List<String> prefixes = objectListing.getCommonPrefixes();
            isDir = (summaries != null && summaries.size() > 0) ||
                    (prefixes != null && prefixes.size() > 0);
        }
        return isDir;
    }

    boolean isDirectorySyntactically(String path) {
        return (path != null) && path.endsWith(UNIX_SEP);
    }

    @Override
    public String normalizePath(String path) {
        String result = null;

        if (path != null) {
            result = path.replace('\\', '/');
        }
        return result;
    }

    AmazonS3 getAuth(Drive drive) {
    	
    	if(amazonS3 != null) {
    		return amazonS3;
    	}
    	
        AmazonS3 client = null;
        if (drive.getPropertyValue(ACCESS_KEY_PROPERTY_KEY) != null) {
            String accessKey = drive.getPropertyValue(ACCESS_KEY_PROPERTY_KEY);
            String secret = drive.getPropertyValue(ACCESS_SECRET_PROPERTY_KEY);
            
            String regionName = null;
            
            if (drive.getPropertyValue(REGION_PROPERTY_KEY) != null) {
                regionName = drive.getPropertyValue(REGION_PROPERTY_KEY);
            }

            if (regionName != null) {
            	client = AmazonS3ClientBuilder
                .standard()
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secret)))
                .withRegion(regionName)
                .build();
            } else {
            	client = AmazonS3ClientBuilder
                .standard()
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secret)))
                .build();
            }

        } else {
            String regionName = null;
            
            if (drive.getPropertyValue(REGION_PROPERTY_KEY) != null) {
                regionName = drive.getPropertyValue(REGION_PROPERTY_KEY);
            }

            if (regionName != null) {
            	client = AmazonS3ClientBuilder
                .standard()
                .withRegion(regionName)
                .build();
            } else {
            	client = AmazonS3ClientBuilder
                .standard()
                .build();
            }
        }

        amazonS3 = client;
        return client;
    }

    S3Client getS3Client(Drive drive) {
    	
    	if(s3Client != null) {
    		return s3Client;
    	}
    	
        S3Client client = null;
        AwsCredentialsProvider credProvider = null;
        software.amazon.awssdk.regions.Region region = null;

        if (drive.getPropertyValue(REGION_PROPERTY_KEY) != null) {
            String regionName = drive.getPropertyValue(REGION_PROPERTY_KEY);
            region = software.amazon.awssdk.regions.Region.of(regionName);
        }

        if (drive.getPropertyValue(ACCESS_KEY_PROPERTY_KEY) != null) {
            String accessKey = drive.getPropertyValue(ACCESS_KEY_PROPERTY_KEY);
            String secret = drive.getPropertyValue(ACCESS_SECRET_PROPERTY_KEY);
            // see https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials.html
            AwsBasicCredentials creds = AwsBasicCredentials.create(accessKey, secret);
            credProvider = StaticCredentialsProvider.create(creds);
        } else {
            credProvider = DefaultCredentialsProvider.create();
        }

        if (region != null) {
            //				String s = org.apache.http.conn.ssl.SSLConnectionSocketFactory.class.getProtectionDomain().getCodeSource().getLocation().getPath();
            client = S3Client.builder().region(region).credentialsProvider(credProvider).build();
        } else {
            client = S3Client.builder().credentialsProvider(credProvider).build();
        }
        
        s3Client = client;
        return client;
    }

    @Override
    public void deleteFile(Drive drive, String path) {
        String bucket = drive.getPropertyValue(BUCKET_NAME_PROPERTY_KEY);
        AmazonS3 s3 = getAuth(drive);
        try {
			Thread.sleep(200);
		} catch (InterruptedException e) {
		}
        s3.deleteObject(bucket, path);
        logger.info("Deleted " + path);
    }

    @Override
    public void deleteDirectory(Drive drive, String prefix) throws IOException {
        DriveQuery query = new DriveQuery();
        query.setStartPath(prefix);
        query.setRecursive(true);

        List<DriveItem> driveItems = find(drive, query);

        // TODO - while from S3's perspective all driveItems are
        // just objects, from a user's prespective, it might be nice
        // to delete in some order, e.g., deepest first
        driveItems.stream()
        .filter(driveItem -> 
        ((driveItem.getPath() != null)
                //							When deleting files from a "directory" (prefix), we don't
                //							want to delete files with a name of the prefix plus an
                //							extension.  For example, if we want to delete the "dir1"
                //							directory, we don't want to delete dir1.zip.
                && !driveItem.getPath().startsWith(prefix + ".")))
        .forEach(driveItem -> deleteFile(drive, driveItem.getPath()));

        //if s3 has a marker directory item, delete it (a path ending in "/" with no object associated with it)
        if (exists(drive, prefix)) {
            deleteFile(drive, prefix);
        }
    }

    @Override
    public void rename(Drive drive, String currentPath, String newPath) throws IOException {
        // There is no rename or mv in AWS S3. Instead, one must copy
        // and then delete the original.
        String bucket = drive.getPropertyValue(BUCKET_NAME_PROPERTY_KEY);

        if (exists(drive, newPath) || isDirectorySemantically(drive, newPath)) {
            throw new FileAlreadyExistsException("Cannot rename file \"" + currentPath + "\" to \"" + newPath + "\"" +
                    " because \"" + newPath + "\" already exists");
        }

        // Because there are no true directories in S3, we must
        // rename everything with the currentPath prefix.
        if (isDirectory(drive, currentPath)) {
            copyDirectory(drive, bucket, currentPath, newPath);
            this.delete(drive, currentPath);
        }
        else { // a single object/file
            moveOneObject(drive, bucket, currentPath, newPath);
        }
    }

    void copyDirectory(Drive drive, String bucket, String currentPath, String newPath) throws IOException {
        DriveQuery query = new DriveQuery(currentPath);
        query.setDriveId(drive.getDriveId());
        query.setRecursive(true);

        List<DriveItem> driveItems = find(drive, query);
        List<DriveItem> keysSorted = driveItems.stream()
                .sorted(Comparator.comparing(DriveItem::getPath))
                .collect(Collectors.toList());

        //ensure the new directory is created first
        mkdir(drive, newPath);

        // Copy the objects in the directory one at a time.
        // An exception may be thrown in the middle of the for loop,
        // in which case there will be a partial copy.  No cleanup
        // is attempted.
        for (DriveItem item : keysSorted) {
            String oldKey = item.getPath();
            String oldTail = oldKey.substring(currentPath.length());
            String newKey = newPath + oldTail;

            if (item.isDirectory()) {
                mkdir(drive, newKey);
            } else {
                CopyObjectRequest request = new CopyObjectRequest(bucket, oldKey, bucket, newKey);
                copyOneObject(drive, request, item);
            }
        }
    }

    public void move(Drive drive, TransferSpec spec) throws IOException {
        String sourcePath = spec.getSourcePath();
        if (sourcePath == null) {
            throw new FileNotFoundException("No source path specified.");
        }
        String destPath = spec.getDestPath();
        if (destPath == null) {
            throw new FileNotFoundException("No destination path specified.");
        }

        String bucket = drive.getPropertyValue(BUCKET_NAME_PROPERTY_KEY);
        String currentPath = spec.getSourcePath();
        String newPath = spec.getDestPath();
        moveOneObject(drive, bucket, currentPath, newPath);
    }

    void moveOneObject(Drive drive, String bucket, String currentPath, String newPath) throws IOException {
        CopyObjectRequest request = new CopyObjectRequest(bucket, currentPath, bucket, newPath);
        // TODO copyObjectRequest.set*
        //		copyObjectUsingTransferManager(s3, copyObjectRequest);
        long startTime = System.currentTimeMillis();

        try {
            copyOneObject(drive, request, null);

            // Only delete the old object if the copy succeeded
            // For now, this a synchronous call. Later, we may need to add
            // some logic to handle asynchronous copies.
            deleteFile(drive, currentPath);
            logger.info("Moved " + currentPath + " to " + newPath);
        } catch (AmazonClientException e) {
            String msg = "Exception while attempting to copy " + currentPath + " to " + newPath;
            logger.warning(msg);
            throw new IOException(msg, e);
        }
        long duration = System.currentTimeMillis() - startTime;
        logger.info("Copy of file to " + request.getDestinationKey() +
                " (took " + duration + " ms).");
    }

    @Override
    public void copy(Drive drive, String sourcePath, String destPath) throws IOException {
        if (sourcePath == null) {
            throw new FileNotFoundException("No source path specified.");
        }
        if (destPath == null) {
            throw new FileNotFoundException("No destination path specified.");
        }

        String bucket = drive.getPropertyValue(BUCKET_NAME_PROPERTY_KEY);
        CopyObjectRequest request = new CopyObjectRequest(bucket, sourcePath , bucket, destPath);

        if (isDirectory(drive, sourcePath)) {
            copyDirectory(drive, bucket, sourcePath, destPath);
        }
        else {
            copyOneObject(drive, request, null);
        }
    }

    void copyOneObject(Drive drive, CopyObjectRequest request, DriveItem previousDriveItem) throws IOException {

        if(previousDriveItem == null) {
            previousDriveItem = getDriveItem(drive, request.getSourceKey());
        }

        if(previousDriveItem != null) {
            StorageClass storageClass = previousDriveItem.getStorageClass();

            if (storageClass != null) {
                request = request.withStorageClass(storageClass.getClassName());
            }
        }

        AmazonS3 auth = getAuth(drive);
        TransferManager manager = 
                TransferManagerBuilder.standard().withS3Client(auth).build();

        try {
			Thread.sleep(200);
		} catch (InterruptedException e) {
		}
        Copy copy = manager.copy(request);
        try {
            copy.waitForCompletion(); // force synchronous behavior
        } catch (AmazonClientException | InterruptedException e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    public boolean exists(Drive drive, String path) {
        boolean result = false;
        path = removeFirstSlash(path);

        if (!isRoot(path)) {
            AmazonS3 s3 = getAuth(drive);
            String bucketName = drive.getPropertyValue(BUCKET_NAME_PROPERTY_KEY);
            result = s3.doesObjectExist(bucketName, removeFirstSlash(path))
                    || isDirectorySemantically(drive, path);
        }
        return result;
    }

    /**
     * Lists the drive items on the drive that match the provided query
     */
    @Override
    public List<DriveItem> find(Drive drive, DriveQuery query) throws ExplorerException {

        AmazonS3 s3 = getAuth(drive);

        String bucketName = drive.getPropertyValue(BUCKET_NAME_PROPERTY_KEY);

        if (bucketName == null) {
            throw new ExplorerException("No bucket name associated with " + drive + ".");
        }

        /*
         * Buckets with many objects might truncate their results when listing their
         * objects, so we're checking to see if the returned object listing is
         * truncated, and using the AmazonS3.listNextBatchOfObjects(...) operation to
         * retrieve additional results.
         */
        ListObjectsRequest listObjectsRequest =
                new ListObjectsRequest().withBucketName(bucketName);

        if (!isRoot(query.getStartPath())) {
            listObjectsRequest = listObjectsRequest.withPrefix(query.getStartPath());
        }

        if (!query.isRecursive()) {
            listObjectsRequest.setDelimiter(PathProcessor.UNIX_SEP);
        }

        ArrayList<DriveItem> driveItems = new ArrayList<>();
        List<S3ObjectSummary> objectSummaries = null;
        ObjectListing objectListing = s3.listObjects(listObjectsRequest);

        // Process each truncated listing
        while (objectListing.isTruncated())  {
            objectSummaries = getObjectSummaries(query, objectListing);
            addDriveItems(drive, query, driveItems, objectSummaries);
            objectListing = s3.listNextBatchOfObjects(objectListing);
        }

        // Handle the final nontruncated listing
        objectSummaries = getObjectSummaries(query, objectListing);
        addDriveItems(drive, query, driveItems, objectSummaries);

        return driveItems;
    }

    /**
     * Gather disk usage data for all of the files on the drive
     * and save it to the database.
     */
    @Override
    public long findAndSaveFileMemoryUsage(Drive drive,
            EntityManager entityManager, Date startDate) throws ExplorerException {
        long totalBytes = 0L;
        long numItems = 0L;
        
        DriveQuery query = new DriveQuery();
        query.setRecursive(true);
        query.setSearchPattern(".*");
        query.setStartPath("");
        query.setUsesPlaceholder(false);
        query.setDriveId(drive.getDriveId());

        AmazonS3 s3 = getAuth(drive);
        String bucketName = drive.getPropertyValue(BUCKET_NAME_PROPERTY_KEY);

        if (bucketName == null) {
            throw new ExplorerException("No bucket name associated with " + drive + ".");
        }

        /*
         * Buckets with many objects might truncate their results when listing their
         * objects, so we're checking to see if the returned object listing is
         * truncated, and using the AmazonS3.listNextBatchOfObjects(...) operation to
         * retrieve additional results.
         */
        ListObjectsRequest listObjectsRequest =
                new ListObjectsRequest().withBucketName(bucketName);

        ArrayList<DriveItem> items = new ArrayList<>();
        List<S3ObjectSummary> objectSummaries = null;
        ObjectListing objectListing = s3.listObjects(listObjectsRequest);
        
        // Process each truncated listing
        while (objectListing.isTruncated())  {
        	
            objectSummaries = getObjectSummaries(query, objectListing);
            addDriveItems(drive, items, objectSummaries);
            totalBytes +=
                    UsageDataCacher.saveFileUsageDataAndClear(entityManager, drive, items, startDate, false);
            numItems += items.size();
            logger.info("Saved " + numItems + " items for " + drive);
            items.clear();
            objectListing = s3.listNextBatchOfObjects(objectListing);

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

        // Handle the final nontruncated listing
        objectSummaries = getObjectSummaries(query, objectListing);
        addDriveItems(drive, items, objectSummaries);
        totalBytes +=
                UsageDataCacher.saveFileUsageDataAndClear(entityManager, drive, items, startDate, true);
        numItems += items.size();
        logger.info("Saved " + numItems + " drive items in " + drive);
        return totalBytes;
    }
    

    private void addDriveItems(Drive drive, DriveQuery query, ArrayList<DriveItem> driveItems,
            List<S3ObjectSummary> objectSummaries) throws ExplorerException {
        int size = objectSummaries.size();

        if (size > 0) {
            List<DriveItem> filtered = buildAndFilterDriveItems(objectSummaries, drive, query);
            driveItems.addAll(filtered);
        }
    }

    private void addDriveItems(Drive drive, ArrayList<DriveItem> driveItems,
            List<S3ObjectSummary> objectSummaries) throws ExplorerException {
        int size = objectSummaries.size();

        if (size > 0) {
            List<DriveItem> items = buildDriveItems(objectSummaries, drive);
            driveItems.addAll(items);
        }
    }

    List<S3ObjectSummary> getObjectSummaries(DriveQuery query, ObjectListing objectListing) {
        List<S3ObjectSummary> objectSummaries = objectListing.getObjectSummaries();
        if (!query.isRecursive()) {
            addCommonPrefixesToSummaries(objectListing, objectSummaries);
        }
        return objectSummaries;
    }

    void addCommonPrefixesToSummaries(ObjectListing objectListing, List<S3ObjectSummary> objectSummaries) {
        for (String prefix : objectListing.getCommonPrefixes()) {
            S3ObjectSummary summary = new S3ObjectSummary();
            summary.setKey(prefix);
            objectSummaries.add(summary);
        }
    }

    @Override
    public BasicFile download(Drive drive, String path) throws IOException {
        AmazonS3 s3 = getAuth(drive);
        String bucket = drive.getPropertyValue(BUCKET_NAME_PROPERTY_KEY);
        GetObjectRequest objectRequest = new GetObjectRequest(bucket, path);
        BasicFile file = FileUtil.buildTempFile("s3_download");
        S3Object fullObject = s3.getObject(objectRequest);
        logger.info("Content-Type: " + fullObject.getObjectMetadata().getContentType());

        TransferManager manager =
                TransferManagerBuilder.standard().withS3Client(s3).build();

        Download download = manager.download(bucket, path, file);
        long startTime = System.currentTimeMillis();
        try {
            download.waitForCompletion();
        } catch (AmazonClientException | InterruptedException e) {
            throw new IOException(e.getMessage(), e);
        }
        // After the upload is complete, release the resources.
        finally {
            manager.shutdownNow();
        }
        long duration = System.currentTimeMillis() - startTime;
        logger.info("Downloaded file to " + path +
                " (took " + duration + " ms).");
        return file;
    }

    @Override
    protected Object[] getMetadataForBuildDriveItem(Drive drive) {
        AmazonS3 auth = getAuth(drive);

        return new Object[]{auth};
    }

    @Override
    protected DriveItem buildDriveItem(S3ObjectSummary objectSummary, Drive drive, Object... args) {
        DriveItem item = new DriveItem();

        AmazonS3 s3 = (AmazonS3) args[0];

        String path = objectSummary.getKey();
        boolean isDirectory = (path != null) && path.endsWith(UNIX_SEP);

        item.setPath(path); // TODO check correctness, look into prefixes
        item.setFileSize(objectSummary.getSize());
        item.setModifiedDate(objectSummary.getLastModified());
        item.setDirectory(isDirectory);
        item.setDriveId(drive.getDriveId());

        if (!isDirectory && _storageClasses.containsKey(objectSummary.getStorageClass())) {
            StorageClass storageClass = _storageClasses.get(objectSummary.getStorageClass());

            if (storageClass.isRestoreRequired()) {
                String bucketName = drive.getPropertyValue(BUCKET_NAME_PROPERTY_KEY);
                ObjectMetadata metadata = s3.getObjectMetadata(bucketName, path);

                boolean isRestoring = Boolean.TRUE.equals(metadata.getOngoingRestore());
                item.setRestoring(isRestoring);

                Date restoreExpirationTime = metadata.getRestoreExpirationTime();
                boolean hasRestoreExpirationTime = (restoreExpirationTime != null);

                //is done restoring and still has time left before restore is expired
                if (!isRestoring && hasRestoreExpirationTime) {
                    String restoreExpireDate =
                            new SimpleDateFormat("MM/dd/yyyy hh:mm").format(restoreExpirationTime);
                    String timeZoneShort = TimeZone.getDefault().getDisplayName(false, TimeZone.SHORT);

                    item.setRestoreExpireDate(restoreExpireDate + " " + timeZoneShort);
                }
            }

            item.setStorageClass(storageClass);
        }

        return item;
    }

    @Override
    public void uploadDirectory(Drive drive, String folderDrivePath, File directory) throws IOException {
        String bucketName = drive.getPropertyValue(BUCKET_NAME_PROPERTY_KEY);
        logger.info("Uploading directory " + directory.getAbsolutePath());
        AmazonS3 s3Client = getAuth(drive);
        TransferManager tm =
                TransferManagerBuilder.standard().withS3Client(s3Client).build();


        //an initial slash will be considered as apart of the folder name; thus, remove it
        folderDrivePath = removeFirstSlash(folderDrivePath);
        MultipleFileUpload upload = tm.uploadDirectory(bucketName,
                folderDrivePath, directory, true);
        try {
            upload.waitForCompletion();
        } catch (AmazonClientException | InterruptedException e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    @Override
    public void upload(Drive drive, String destPath, File file) throws IOException {
        if (exists(drive, destPath)) {
            throw new FileAlreadyExistsException("Attempted to upload \"" + destPath + "\", but it already exists");
        }

        String bucket = drive.getPropertyValue(BUCKET_NAME_PROPERTY_KEY);
        AmazonS3 s3 = getAuth(drive);

        if (destPath.startsWith(UNIX_SEP)) {
            destPath = destPath.substring(1);
        }
        ensureDirsExist(drive, destPath);

        uploadFileUsingTransferManager(s3, bucket, destPath, file);
        logger.info("Uploaded file to " + destPath);
    }

    void uploadFileUsingTransferManager(AmazonS3 s3, String bucket,
            String destPath, File file) throws IOException {
        TransferManager manager =
                TransferManagerBuilder.standard().withS3Client(s3).build();
        // default multiPartUploadThreshold =  16_777_216
        // default multiPartUploadPartSize =    5_242_880
        // default multiPartCopyThreshold = 5_368_709_120
        // default multiPartCopyPartSize =    104_857_600
        Upload myUpload = manager.upload(bucket, destPath, file);

        long startTime = System.currentTimeMillis();
        try {
            myUpload.waitForCompletion();
        } catch (AmazonClientException | InterruptedException e) {
            throw new IOException(e.getMessage(), e);
        }
        // After the upload is complete, release the resources.
        finally {
            manager.shutdownNow();
        }
        long duration = System.currentTimeMillis() - startTime;
        logger.info("Uploaded file to " + destPath +
                " (took " + duration + " ms).");
    }

    /**
     * Make sure the dirs containing the file exist,
     * creating them if necessary
     * @param drive
     * @param destPath
     * @throws IOException 
     */
    protected void ensureDirsExist(Drive drive, String destPath) throws IOException {
        destPath = normalizePath(destPath);
        int nextIndex = destPath.indexOf(UNIX_SEP);
        String nextDir = ""; // the next directory to check/create

        while (nextIndex > 0) {
            nextDir = destPath.substring(0, nextIndex);
            String prefix = getPathForPrefix(nextDir);
            if (!exists(drive, prefix)) {
                mkdir(drive, prefix);
            }
            nextIndex = destPath.indexOf(UNIX_SEP, nextIndex + 1);
        }
    }


    @Override
    public void mkdir(Drive drive, String path) throws IOException {
        // create meta-data for your folder and set content-length to 0
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(0);

        // create empty content
        InputStream emptyContent = new ByteArrayInputStream(new byte[0]);

        String bucket = drive.getPropertyValue(BUCKET_NAME_PROPERTY_KEY);
        String pathForPrefix = getPathForPrefix(path);

        if (exists(drive, pathForPrefix)) {
            logger.warning("Mkdir: " + pathForPrefix + " already exists.");
            throw new FileAlreadyExistsException("Attempted to create folder at path \"" + pathForPrefix + "\", but a folder already exists.");
        }

        // create a PutObjectRequest passing the folder name suffixed by /
        PutObjectRequest putObjectRequest =
                new PutObjectRequest(
                        bucket,
                        pathForPrefix,
                        emptyContent, metadata);

        // send request to S3 to create folder
        AmazonS3 s3 = getAuth(drive);
        s3.putObject(putObjectRequest);
    }

    /**
     * Make sure the prefix has no initial "/", but
     * does have a terminal "/".
     * @param path
     * @return the path for the prefix
     */
    private String getPathForPrefix(String path) {
        String prefixPath = PathProcessor.removeFirstSlash(path);
        return PathProcessor.addLastSlash(prefixPath);
    }

    /** Used for archiving data that rarely needs to be accessed. */
    public static final String DEEP_ARCHIVE = "DEEP_ARCHIVE";

    /** Used for archives where portions of the data might need to
     *  be retrieved in minutes. */
    public static final String GLACIER = "GLACIER";

    /** For objects larger than 128 KB that you plan to store
     *  for at least 30 days. */
    public static final String INTELLIGENT_TIERING = "INTELLIGENT_TIERING";

    /** Use if you can re-create the data if the Availability Zone fails,
     *  and for object replicas when setting S3 Cross-Region
     *  Replication (CRR). */
    public static final String ONEZONE_IA = "ONEZONE_IA";

    /** For noncritical, reproducible data that can be stored
     *  with less redundancy than the S3 Standard storage class. */
    public static final String REDUCED_REDUNDANCY = "REDUCED_REDUNDANCY";

    /** Used for performance-sensitive use cases (those that require
     *  millisecond access time) and frequently accessed data. */
    public static final String STANDARD = "STANDARD";

    /** Used for your primary or only copy of data that can't be re-created. */
    public static final String STANDARD_IA = "STANDARD_IA";

    final HashMap<String, StorageClass> _storageClasses = new HashMap<String, StorageClass>() {{
        put(STANDARD, new StorageClass(STANDARD, "Standard", false, true));
        put(STANDARD_IA, new StorageClass(STANDARD_IA, "Standard-IA", false));
        put(ONEZONE_IA, new StorageClass(ONEZONE_IA, "OneZone-IA", false));
        put(INTELLIGENT_TIERING, new StorageClass(INTELLIGENT_TIERING, "Intelligent", false));
        put(GLACIER, new StorageClass(GLACIER, "Glacier", true));
        put(DEEP_ARCHIVE, new StorageClass(DEEP_ARCHIVE, "Deep Archive", true));
        put(REDUCED_REDUNDANCY, new StorageClass(REDUCED_REDUNDANCY, "Reduced Redundancy", false));
    }};

    @Override
    public List<StorageClass> getStorageClasses() {
        return new ArrayList<StorageClass>(_storageClasses.values());
    }

    @Override
    public void updateStorageClass(Drive drive, String path, StorageClass storageClass)
            throws IOException {
        String bucket = drive.getPropertyValue(BUCKET_NAME_PROPERTY_KEY);

        AmazonS3 s3Client = getAuth(drive);
        TransferManager transferManager =
                TransferManagerBuilder.standard().withS3Client(s3Client).build();

        final CopyObjectRequest copyRequest =
                new CopyObjectRequest(bucket, path, bucket, path)
                .withStorageClass(storageClass.getClassName());

        // attempt to refresh existing object in the bucket via an inplace copy
        Copy copy = transferManager.copy(copyRequest);

        try {
            copy.waitForCompletion();
        } catch (AmazonClientException | InterruptedException e) {
            throw new IOException("Was unable to change the storage class of drive item \"" + path + "\" to " + storageClass, e);
        }
    }

    @Override
    public void restore(Drive drive, String path, int daysExpiration)
            throws IOException {
        if (isDirectory(drive, path)) {
            List<DriveItem> driveItems = findAllInPath(drive, path);

            for (DriveItem driveItem : driveItems) {
                boolean isNotRestoring = !driveItem.isRestoring();

                if (driveItem.isFile() && driveItem.isRestoreRequired() && isNotRestoring) {
                    restoreOne(drive, driveItem.getPath(), daysExpiration);
                }
            }
        } else {
            restoreOne(drive, path, daysExpiration);
        }
    }

    @Override
    public boolean requiresDaysToExpire() {
        return true;
    }

    private void restoreOne(Drive drive, String path, int daysExpiration) throws IOException {
        try {
            String bucketName = drive.getPropertyValue(BUCKET_NAME_PROPERTY_KEY);
            AmazonS3 s3Client = getAuth(drive);

            // Create and submit a request to restore an object from Glacier for "daysExpiration" number of days
            RestoreObjectRequest requestRestore = new RestoreObjectRequest(bucketName, path, daysExpiration);
            s3Client.restoreObjectV2(requestRestore);

            // Check the restoration status of the object.
            //			ObjectMetadata response = s3Client.getObjectMetadata(bucketName, path);

            //this flag tests if there is an ongoing restore currently, NOT if it is a success/failure
            //			Boolean restoreFlag = response.getOngoingRestore();
        } catch (AmazonServiceException e) {
            throw new IOException("The call was transmitted successfully, but Amazon S3 threw an error.", e);
        }
    }

    @Override
    public void downloadPartStart(Drive drive, String path) throws IOException {
        String bucket = drive.getPropertyValue(BUCKET_NAME_PROPERTY_KEY);
        s3Client = getS3Client(drive);
        headObjectRequest =
                HeadObjectRequest.builder()
                .bucket(bucket)
                .key(path)
                .build();
        headObjectResponse =
                s3Client.headObject(headObjectRequest);
    }

    @Override
    public byte[] downloadBytes(Drive drive, String path, long startByte, int numberOfBytes) throws IOException {
        long start = System.currentTimeMillis();
        byte[] result = null;
        String bucket = drive.getPropertyValue(BUCKET_NAME_PROPERTY_KEY);

        long endByte = startByte + numberOfBytes - 1;
        Long contentLength = headObjectResponse.contentLength();

        if (contentLength == 0) {
            result = new byte[0];
        }
        else {
            // For more information about the HTTP Range header, go to
            // http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html#sec14.35.
            // Should be something like: "bytes=0-499"
            String sRange = "bytes=" + startByte + "-" + endByte;

            logger.info("Downloading " + sRange + " from " + path);
            software.amazon.awssdk.services.s3.model.GetObjectRequest objectRequest =
                    software.amazon.awssdk.services.s3.model.GetObjectRequest
                    .builder()
                    .key(path)
                    .bucket(bucket)
                    .range(sRange)
                    .build();

            ResponseBytes<GetObjectResponse> objectBytes =
                    s3Client.getObjectAsBytes(objectRequest);
            byte[] bytes = objectBytes.asByteArray();

            if (bytes == null) {
                result = new byte[0];
            } else if (bytes.length < numberOfBytes) {
                result = Arrays.copyOfRange(bytes, 0, bytes.length);
            } else {
                result = bytes;
            }
            logger.info("Download of " + result.length + " bytes took " +
                    (System.currentTimeMillis() - start) + " ms.");
        }
        return result;
    }

    @Override
    public String uploadPartStart(Drive drive, String path) throws IOException {
        String bucketName = drive.getPropertyValue(BUCKET_NAME_PROPERTY_KEY);
        multipartUploadRequest = CreateMultipartUploadRequest.builder()
                .bucket(bucketName)
                .key(path)
                .build();
        s3Client = getS3Client(drive);
        response = s3Client.createMultipartUpload(multipartUploadRequest);
        String uploadId = response.uploadId();
        return uploadId;
    }

    /**
     * @param partNumber a part number between 1 and 10,000
     * @throws IOException
     * @see https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html
     */
    @Override
    public void uploadPart(Drive drive, String path, byte[] data, int partNumber) throws IOException {
        logger.info("Uploading part " + partNumber + " of " + path);
        String bucketName = drive.getPropertyValue(BUCKET_NAME_PROPERTY_KEY);
        long start = System.currentTimeMillis();
        UploadPartRequest uploadPartRequest = UploadPartRequest.builder()
                .bucket(bucketName)
                .key(path)
                .uploadId(response.uploadId())
                .partNumber(partNumber).build();

        String etag = s3Client.uploadPart(uploadPartRequest, RequestBody.fromBytes(data)).eTag();
        CompletedPart part = CompletedPart.builder().partNumber(partNumber).eTag(etag).build();
        logger.info("Upload of " + data.length + " bytes took " +
                (System.currentTimeMillis() - start ) + " ms.");
        completedParts.add(part);
    }

    /**
     * @param partNumber a part number between 1 and 10,000
     * @throws IOException
     * @see https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html
     */
    @Override
    public void uploadPartComplete(Drive drive, String path, String uploadId) throws IOException {
        String bucketName = drive.getPropertyValue(BUCKET_NAME_PROPERTY_KEY);

        if (completedParts != null && !completedParts.isEmpty()) {
            // Call completeMultipartUpload operation to tell S3 to merge all uploaded
            // parts and finish the multipart operation.
            CompletedMultipartUpload completedMultipartUpload =
                    CompletedMultipartUpload.builder()
                    .parts(completedParts)
                    .build();

            CompleteMultipartUploadRequest completeMultipartUploadRequest =
                    CompleteMultipartUploadRequest.builder()
                    .bucket(bucketName).key(path)
                    .uploadId(uploadId)
                    .multipartUpload(completedMultipartUpload)
                    .build();

            s3Client.completeMultipartUpload(completeMultipartUploadRequest);
            logger.info("Upload complete for file " + path);
        }
        // No parts, empty file
        else {
            // create meta-data and set content-length to 0
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(0);

            // create empty content
            InputStream emptyContent = new ByteArrayInputStream(new byte[0]);

            String bucket = drive.getPropertyValue(BUCKET_NAME_PROPERTY_KEY);
            PutObjectRequest putObjectRequest =
                    new PutObjectRequest(bucket, path, emptyContent, metadata);

            // send request to S3 to create empty file
            AmazonS3 s3 = getAuth(drive);
            s3.putObject(putObjectRequest);
            logger.info("Upload complete for empty file " + path);
        }
        // reset for the next chunk upload
        completedParts = new ArrayList<>();
        response = null;
    }

    /**
     * This action aborts a multipart upload. After a multipart upload
     *  is aborted, no additional parts can be uploaded using that upload ID.
     *  The storage consumed by any previously uploaded parts will be freed.
     *  However, if any part uploads are currently in progress,
     *  those part uploads might or might not succeed.
     *  As a result, it might be necessary to abort a given multipart upload
     *  multiple times in order to completely free all storage consumed by all parts.

		To verify that all parts have been removed, so you don't get charged
		for the part storage, you should call the ListParts action and ensure
		that the parts list is empty.

		For information about permissions required to use the multipart upload,
		see Multipart Upload and Permissions.
     * @param drive
     * @param path
     * @param uploadId
     * @see https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html
     * @see https://docs.aws.amazon.com/AmazonS3/latest/API/API_AbortMultipartUpload.html
     * @throws IOException
     */
    @Override
    public void uploadPartAbort(Drive drive, String path, String uploadId) throws IOException {
        String bucketName = drive.getPropertyValue(BUCKET_NAME_PROPERTY_KEY);

        // Call completeMultipartUpload operation to tell S3 to merge all uploaded
        // parts and finish the multipart operation.
        AbortMultipartUploadRequest abortRequest =
                AbortMultipartUploadRequest.builder()
                .bucket(bucketName)
                .key(path)
                .uploadId(uploadId)
                .build();
        s3Client.abortMultipartUpload(abortRequest);

        // reset for the next chunk upload
        completedParts = new ArrayList<>();
        response = null;
    }

    @Override
    public String getHiveLocationPath(Drive drive, String sourcePath) {

        String parentFolder = getParentFolderPath(sourcePath);
        return "s3a://" + drive.getPropertyValue(BUCKET_NAME_PROPERTY_KEY) + "/" + parentFolder;
        //return "s3a://mdaca-storage-1/Mediciation";
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
        AmazonS3 s3 = getAuth(drive);
        String bucketName = drive.getPropertyValue(BUCKET_NAME_PROPERTY_KEY);
        ListObjectsRequest listObjectsRequest = new ListObjectsRequest().withBucketName(bucketName);
        listObjectsRequest = listObjectsRequest.withPrefix(path);
        ObjectListing objectListing = s3.listObjects(listObjectsRequest);

        List<S3ObjectSummary> objectSummaries = objectListing.getObjectSummaries();
        Optional<S3ObjectSummary> oSummary = objectSummaries.stream()
                .filter(s -> s.getKey().equals(path)).findFirst();
        BasicFile tempFile = null;

        if (oSummary.isPresent()) {
            S3ObjectSummary summary = oSummary.get();
            DriveItem driveItem = buildDriveItem(summary, drive, s3, bucketName);
            tempFile = getTopLinesFromDriveItem(drive, path, numLines, driveItem);
        }
        return tempFile;
    }

    public InputStream getInputStream(Drive drive, String path) throws IOException {
        AmazonS3 s3 = getAuth(drive);
        String bucket = drive.getPropertyValue(BUCKET_NAME_PROPERTY_KEY);
        GetObjectRequest objectRequest = new GetObjectRequest(bucket, path);
        S3Object object = s3.getObject(objectRequest);
        return object.getObjectContent();
    }

    @Override
    public List<String> getProperties() {
        return Arrays.asList(
                REGION_PROPERTY_KEY,
                ACCESS_KEY_PROPERTY_KEY,
                ACCESS_SECRET_PROPERTY_KEY,
                BUCKET_NAME_PROPERTY_KEY,
                HIVE_HOST_NAME,
                HIVE_PORT);
    }

    @Override
    public boolean testConnection(Drive drive) throws ExplorerException {
        boolean exists = false;

        try {
            AmazonS3 client = getAuth(drive);
            String bucketName = drive.getPropertyValue(BUCKET_NAME_PROPERTY_KEY);
            List<Bucket> buckets = client.listBuckets();
            exists = buckets.stream().anyMatch(
                    b -> b.getName().equalsIgnoreCase(bucketName));
        } catch (Exception e) {
            throw new ExplorerException("Exception while testing connection to " +
                    drive + ". " + e.getMessage(), e);
        }
        return exists;
    }


}
