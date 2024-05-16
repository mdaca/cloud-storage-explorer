package com.spinsys.mdaca.storage.explorer.provider;

import static com.spinsys.mdaca.storage.explorer.io.PathProcessor.removeFirstSlash;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.security.auth.Subject;
import javax.servlet.http.HttpServletRequest;

import org.apache.commons.io.IOUtils;
import org.ietf.jgss.GSSException;

import com.hierynomus.msdtyp.AccessMask;
import com.hierynomus.msfscc.FileAttributes;
import com.hierynomus.msfscc.fileinformation.FileAllInformation;
import com.hierynomus.msfscc.fileinformation.FileBasicInformation;
import com.hierynomus.msfscc.fileinformation.FileIdBothDirectoryInformation;
import com.hierynomus.mssmb2.SMB2CreateDisposition;
import com.hierynomus.mssmb2.SMB2CreateOptions;
import com.hierynomus.mssmb2.SMB2ShareAccess;
import com.hierynomus.mssmb2.SMBApiException;
import com.hierynomus.smbj.SMBClient;
import com.hierynomus.smbj.SmbConfig;
import com.hierynomus.smbj.auth.AuthenticationContext;
import com.hierynomus.smbj.auth.GSSAuthenticationContext;
import com.hierynomus.smbj.connection.Connection;
import com.hierynomus.smbj.io.InputStreamByteChunkProvider;
import com.hierynomus.smbj.session.Session;
import com.hierynomus.smbj.share.DiskEntry;
import com.hierynomus.smbj.share.DiskShare;
import com.hierynomus.smbj.share.File;
import com.spinsys.mdaca.storage.explorer.io.BasicFile;
import com.spinsys.mdaca.storage.explorer.io.FileUtil;
import com.spinsys.mdaca.storage.explorer.io.PathProcessor;
import com.spinsys.mdaca.storage.explorer.model.DriveItem;
import com.spinsys.mdaca.storage.explorer.model.DriveQuery;
import com.spinsys.mdaca.storage.explorer.model.exception.ExplorerException;
import com.spinsys.mdaca.storage.explorer.persistence.Drive;

import net.sourceforge.spnego.SpnegoPrincipal;

public class SMBStorageProvider extends BasicStorageProvider {

    private static final Logger logger =
            Logger.getLogger("com.spinsys.mdaca.storage.explorer.provider.SMBStorageProvider");

    /** SMB */
    public static final String HOST_NAME_PROPERTY_KEY = "HostName";
    public static final String DOMAIN_PROPERTY_KEY = "Domain";
    public static final String USER_NAME_PROPERTY_KEY = "UserName";
    public static final String PASSWORD_PROPERTY_KEY = "Password";
    public static final String SHARE_NAME_PROPERTY_KEY = "ShareName";

    public SMBStorageProvider() {
    }

    public SMBStorageProvider(HttpServletRequest request) {
        this.principal = (SpnegoPrincipal)request.getAttribute("spnegoprin");
    }

    public SMBStorageProvider(SpnegoPrincipal principal) {
        this.principal = principal;
    }

    Subject subject;
    SpnegoPrincipal principal;
    @Override
    public boolean isDirectory(Drive drive, String path) {
        if (drive == null) {
            return false;
        }

        String smbPath = path;

        if(smbPath.endsWith(PathProcessor.GUI_SEP)) {
            smbPath = smbPath.substring(0, smbPath.length() - 1);
        }

        boolean isDir = false;
        SMBClient client = new SMBClient();

        String server = drive.getPropertyValue(HOST_NAME_PROPERTY_KEY); // "SSLW-01585"; // "192.168.1.233";
        String shareName = drive.getPropertyValue(SHARE_NAME_PROPERTY_KEY); // "SMB_Share";

        // TODO This is expensive.  Anything simpler? cache the connection
        try (Connection connection = client.connect(server);
                Session session = connection.authenticate(getAuth(drive)))
        {
            try (DiskShare share = (DiskShare) session.connectShare(shareName)) {
                isDir = share.folderExists(smbPath);
            }
            catch (SMBApiException e) {
                logger.log(Level.WARNING, e.getMessage(), e);
            }
        } catch (IOException e) {
            logger.log(Level.WARNING, e.getMessage(), e);
        } finally {
            client.close();
        }
        return isDir;
    }

    @Override
    public DriveItem getDriveItem(Drive drive, String path) throws ExplorerException {
        DriveItem item = null;
        path = getPathWithDriveLetter(drive, path);

        try {
            SMBClient client = new SMBClient();

            String server = drive.getPropertyValue(HOST_NAME_PROPERTY_KEY); //
            String shareName = drive.getPropertyValue(SHARE_NAME_PROPERTY_KEY); // "SMB_Share";

            try (Connection connection = client.connect(server);
                    Session session = connection.authenticate(getAuth(drive)))
            {
                try (DiskShare share = (DiskShare) session.connectShare(shareName)) {
                    path = normalizeFolderName(path);
                    FileAllInformation info = share.getFileInformation(path);
                    item = populateDriveItem(drive, info, path);
                }
            }
            finally {
                client.close();
            }
        } catch (Exception e) {
            logger.log(Level.WARNING, e.getMessage(), e);
        }
        return item;
    }


    /**
     * @throws GSSException
     * @see https://github.com/hierynomus/smbj
     */
    @Override
    public List<DriveItem> find(Drive drive, DriveQuery query) throws ExplorerException {
        List<DriveItem> driveItems = new ArrayList<>();
        try {
            SMBClient client = new SMBClient();

            String server = drive.getPropertyValue(HOST_NAME_PROPERTY_KEY); //
            String shareName = drive.getPropertyValue(SHARE_NAME_PROPERTY_KEY); // "SMB_Share";
            String folder = query.getStartPath();
            String pattern = query.getSearchPattern();

            try (Connection connection = client.connect(server);
                    Session session = connection.authenticate(getAuth(drive)))
            {
                try (DiskShare share = (DiskShare) session.connectShare(shareName)) {
                    folder = normalizeFolderName(folder);
                    FileAllInformation folderInformation = share.getFileInformation(folder);

                    // If a recursive query, the folder 
                    // itself may go in the drive item list
                    boolean isRecursive = query.isRecursive();
                    if (isRecursive) {
                        String searchPattern = normalizeSearchPattern(pattern);

                        if (folder != null && folder.matches(searchPattern)) {
                            DriveItem startFolderDriveItem = populateDriveItem(drive, folderInformation, folder);
                            driveItems.add(startFolderDriveItem);
                        }
                    }

                    // add descendents to the drive item list
                    collectDriveItemsInFolder(drive, driveItems, folder, pattern, share, isRecursive);
                }
            } finally {
                client.close();
            }
        } catch (Exception e) {
            logger.log(Level.WARNING, e.getMessage(), e);
        }
        return driveItems;
    }
    
    @Override
    public boolean testConnection(Drive drive) throws ExplorerException {
        boolean exists = false;
        SMBClient client = new SMBClient();

        String server = drive.getPropertyValue(HOST_NAME_PROPERTY_KEY);
        String shareName = drive.getPropertyValue(SHARE_NAME_PROPERTY_KEY);

        try (Connection connection = client.connect(server))
        {
            if (connection == null) {
                throw new ExplorerException(
                        "Exception while testing connection to " +
                        drive + ".  Unable to open connection to " + server);
            }
            else {  // got a connection
                AuthenticationContext auth = getAuth(drive);

                if (auth == null) {
                    throw new ExplorerException(
                            "Exception while testing connection to " +
                                    drive + ".  Unable to get authentication context.");
                }
                else {  // got an authentication context
                    try (Session session = connection.authenticate(auth)) {
                        if (session == null) {
                            throw new ExplorerException(
                                    "Exception while testing connection to " +
                                    drive + ".  Unable to get authentication session using " + auth);
                        }
                        else {  // got a session
                            try (DiskShare share = (DiskShare) session.connectShare(shareName)) {
                                exists = (share != null);
                            }
                        }
                    }
                }
            }
        }
        catch (Exception e) {
            throw new ExplorerException("Exception while testing connection to " +
                                        drive + ". " + e.getMessage(), e);
        } finally {
            client.close();
        }
        return exists;
    }

    void collectDriveItemsInFolder(Drive drive, List<DriveItem> driveItems, String folder, String pattern,
            DiskShare share, boolean recurse)
    {
        String searchFolder = normalizeFolderName(folder);
        String searchPattern = normalizeSearchPattern(pattern);
        List<FileIdBothDirectoryInformation> listing =
                share.list(searchFolder);

        for (FileIdBothDirectoryInformation info : listing) {
            String fileName = info.getFileName();

            if (!isSpecialDir(fileName)) { // ignore . and ..
                String fullPath =
                        ("" == searchFolder) ? fileName
                                : searchFolder + PathProcessor.GUI_SEP + fileName;

                if (fullPath.startsWith(PathProcessor.GUI_SEP)) {
                    fullPath = fullPath.substring(1);
                }

                // Only match the file name at this level,
                // not the entire path
                if (fileName.equals(searchPattern) || fileName.matches(searchPattern)) {
                    DriveItem item = populateDriveItem(drive, info, fullPath);
                    driveItems.add(item);
                }

                // Search all subdirectories for matches
                if (recurse) {
                    if ((info.getFileAttributes() & FileAttributes.FILE_ATTRIBUTE_DIRECTORY.getValue()) != 0) {
                        collectDriveItemsInFolder(drive, driveItems, fullPath, pattern, share, recurse);
                    }
                }
            }
        }	// for
    }

    String normalizeFolderName(String folder) {
        String searchFolder = (folder == null) ? "" : folder;
        searchFolder = removeSlashesFromFolderName(searchFolder);
        return searchFolder;
    }

    String removeSlashesFromFolderName(String searchFolder) {
        searchFolder = PathProcessor.removeFirstSlash(searchFolder);
        searchFolder = PathProcessor.removeLastSlash(searchFolder);
        return searchFolder;
    }

    String normalizeSearchPattern(String pattern) {
        // TODO - there is different regex matching behavior
        // on different platforms.  We need to define what our regex
        // matching does.  I suggest we follow Java's.
        String searchPattern = ".*";

        if (pattern != null && !pattern.equals("")) {
            searchPattern = pattern;
        }
        return searchPattern;
    }

    /**
     * Gets all the folder contents that match the searchPattern.
     * @param share
     * @param folder
     * @param pattern
     * @return
     */
    public List<FileIdBothDirectoryInformation> getMatchingFilesInFolder(DiskShare share, String folder,
            String pattern) {
        // This filtering is done remotely
        List<FileIdBothDirectoryInformation> matchingInfo =
                share.list(folder, pattern);
        return matchingInfo;
    }

    DriveItem populateDriveItem(Drive drive, FileIdBothDirectoryInformation info, String fileName) {
        DriveItem item = new DriveItem();
        item.setDriveId(drive.getDriveId());
        item.setPath(fileName);
        item.setFileSize(info.getEndOfFile());
        item.setModifiedDate(info.getChangeTime().toDate());
        item.setDirectory((info.getFileAttributes() &
                FileAttributes.FILE_ATTRIBUTE_DIRECTORY.getValue()) != 0);
        if (item.isDirectory()) {
            item.setPath(item.getPath() + PathProcessor.GUI_SEP);
        }
        return item;
    }

    DriveItem populateDriveItem(Drive drive, FileAllInformation allInfo, String fileName) {
        DriveItem item = new DriveItem();
        FileBasicInformation info = allInfo.getBasicInformation();
        item.setDriveId(drive.getDriveId());
        item.setPath(fileName);
        item.setFileSize(allInfo.getStandardInformation().getEndOfFile());
        item.setModifiedDate(info.getChangeTime().toDate());
        item.setDirectory((info.getFileAttributes()
                & FileAttributes.FILE_ATTRIBUTE_DIRECTORY.getValue()) != 0);
        if (item.isDirectory()) {
            item.setPath(item.getPath() + PathProcessor.GUI_SEP);
        }
        return item;
    }

    AuthenticationContext getAuth(Drive drive) {

        String user = drive.getPropertyValue(USER_NAME_PROPERTY_KEY);
        String pwd = drive.getPropertyValue(PASSWORD_PROPERTY_KEY);
        String shareDomain = drive.getPropertyValue(DOMAIN_PROPERTY_KEY);

        if(user != null && user.length() > 0 && pwd != null && pwd.length() > 0) {
            return new AuthenticationContext(user, pwd.toCharArray(), shareDomain);

        } else if(this.principal != null) {
            subject = new Subject();
            subject.getPrincipals().add(principal);

            logger.log(Level.INFO, "Using delegation principal: " + principal.getName() + ", realm: " + principal.getRealm());

            return new GSSAuthenticationContext(
                    principal.getName(),
                    principal.getRealm(),
                    subject,
                    principal.getDelegatedCredential()
                    );
        }

        return new AuthenticationContext("", new char[0], "");
        
        
    }

    /**
     * Download multiple files from an SMB directory
     * @param drive
     * @param shareSourceDir
     * @param destDir
     * @throws IOException
     * @throws FileNotFoundException
     */
    public List<DriveItem> downloadOne(Drive drive, String shareSourceDir, String destFileName) throws FileNotFoundException, IOException {
        String hostName = drive.getPropertyValue(HOST_NAME_PROPERTY_KEY);
        String sourceDriveName = drive.getPropertyValue(SHARE_NAME_PROPERTY_KEY); // e.g."SMB_Share");

        List<DriveItem> downloads = new ArrayList<>();
        SMBClient client = configureSMB();

        try (Connection connection = client.connect(hostName);
                Session session = connection.authenticate(getAuth(drive)))
        {
            // Connect to a shared folder
            DiskShare share = (DiskShare) session.connectShare(sourceDriveName);
            //                      FileIdBothDirectoryInformation destFile; // TODO FileAllInformation?
            //                      FileAllInformation destFile; // TODO FileAllInformation?

            String sourcePath = shareSourceDir;
            //                      String destPath = /* "C:\\Temp\\MDACA\\" + */ destFileName;

            // filePath should not include the share, e.g., it should be
            // "Temp\smallFile.txt"
            if (fileExists(share, sourcePath)) {
                java.io.File file = downloadFile(share, sourcePath);
                DriveItem newItem = new DriveItem();
                // TODO                                         newItem.setDriveId(driveId);
                newItem.setModifiedDate(new Date());
                newItem.setPath(file.getAbsolutePath());
                // TODO or original date or actual local file date?
                downloads.add(newItem);
            } else {
                logger.info("Unable to download " + sourcePath + ".  File does not exist.");
            }
        } finally {
            if (client != null) {
                client.close();
            }
        }
        return downloads;
    }

    /**
     * Download multiple files from an SMB directory
     * @param drive
     * @param shareSourceDir
     * @param destDir
     */
    public List<DriveItem> downloadMultiple(Drive drive, String shareSourceDir, String destDir) {
        List<DriveItem> downloads = new ArrayList<>();

        return downloads;
    }

    /**
     * Download a file from an SMB directory
     *
     * @param drive
     * @param shareSourceDir
     * @param destDir
     */
    BasicFile downloadFile(DiskShare share, String sourcePath) throws IOException {
        BasicFile tempFile = FileUtil.buildTempFile("SMB_downloadFile");
        FileOutputStream fos = new FileOutputStream(tempFile);
        try (BufferedOutputStream bos = new BufferedOutputStream(fos)) {
            logger.info("Downloading SMB file:" + sourcePath);

            File smbFileRead = share.openFile(sourcePath, EnumSet.of(AccessMask.GENERIC_READ), null,
                    SMB2ShareAccess.ALL, SMB2CreateDisposition.FILE_OPEN, null);
            try (InputStream in = smbFileRead.getInputStream()) {
                byte[] buffer = new byte[4096];
                int len = 0;
                while ((len = in.read(buffer, 0, buffer.length)) != -1) {
                    bos.write(buffer, 0, len);
                }
            }
        }
        logger.info("SMB file download from " + sourcePath +
                " to " + tempFile.getAbsolutePath() + " succeeded");
        return tempFile;
    }


    private static boolean isSpecialDir(String fileName) {
        return fileName.equals(".") || fileName.equals("..");
    }

    @Override
    public void upload(Drive drive, String path, java.io.File inputFile) throws IOException {
        if (exists(drive, path)) {
            throw new FileAlreadyExistsException("Attempted to upload \"" + path + "\", but it already exists");
        }

        String destPath = path;// spec.getDestinationPath();
        String hostName = drive.getPropertyValue(HOST_NAME_PROPERTY_KEY);
        String smbDriveName = drive.getPropertyValue(SHARE_NAME_PROPERTY_KEY); // e.g."SMB_Share");

        SMBClient client = configureSMB();
        Connection connection = client.connect(hostName);
        Session session = connection.authenticate(getAuth(drive));

        // Connect to a shared folder
        DiskShare share = (DiskShare) session.connectShare(smbDriveName);

        if (destPath.startsWith(PathProcessor.GUI_SEP) || destPath.startsWith(PathProcessor.WINDOWS_SEP)) {
            destPath = destPath.substring(1);
        }

        com.hierynomus.smbj.share.File file = null;

        // Create file using flag FILE_CREATE, which will throw if file exists
        // already
        ensureDirsExist(share, destPath); // may create a dir at destPath

        if (!isDirectory(drive, destPath)) {
            logger.info("About to upload " + path + " to " + destPath + ".");
            file = share.openFile(destPath,
                    Collections.singleton(AccessMask.MAXIMUM_ALLOWED),
                    Collections.singleton(FileAttributes.FILE_ATTRIBUTE_NORMAL),
                    SMB2ShareAccess.ALL,
                    SMB2CreateDisposition.FILE_CREATE,
                    Collections.singleton(SMB2CreateOptions.FILE_DIRECTORY_FILE));
        }

        if (file != null) {
            try (InputStream reader = new BufferedInputStream(new FileInputStream(inputFile));
                    OutputStream writer = new BufferedOutputStream(file.getOutputStream())) {

                int read = -1;

                while ((read = reader.read()) != -1) {
                    writer.write(read);
                }
                logger.info("Upload of " + path + " to " + destPath + " complete.");

            } finally {
                if (file != null) {
                    file.close();
                }
            }
        }
    }

    boolean exists(DiskShare share, String path) {
        return fileExists(share, path) || folderExists(share, path);
    }

    boolean fileExists(DiskShare share, String path) {
        boolean exists = false;
        try {
            exists = share.fileExists(path);
        }
        // This is to work around an apparent bug in SMBJ
        catch (SMBApiException e) {
            logger.log(Level.INFO, "Unable to determine whether " +
                    path + " exists - " + e.getMessage());
        }
        return exists;
    }

    boolean folderExists(DiskShare share, String path) {
        boolean exists = false;
        try {
            exists = share.folderExists(path);
        }
        // This is to work around an apparent bug in SMBJ
        catch (SMBApiException e) {
            logger.log(Level.INFO, "Unable to determine whether " +
                    path + " exists - " + e.getMessage());
        }
        return exists;
    }

    /**
     * Make sure the dirs containing the file exist,
     * creating them if necessary
     * @param share
     * @param destPath
     */
    protected void ensureDirsExist(DiskShare share, String destPath) {
        // TODO checking separators should be done elsewhere
        destPath = normalizePath(destPath);
        int nextIndex = destPath.indexOf(PathProcessor.WINDOWS_SEP);
        String nextDir = ""; // the next directory to check/create

        while (nextIndex > 0) {
            nextDir = destPath.substring(0, nextIndex);
            if (!share.folderExists(nextDir)) {
                share.mkdir(nextDir);
            }
            nextIndex = destPath.indexOf(PathProcessor.WINDOWS_SEP, nextIndex + 1);
        }
    }

    @Override
    public String normalizePath(String path) {
        String nPath = path;
        if (path != null) {
            nPath = path.replace(PathProcessor.GUI_SEP, PathProcessor.WINDOWS_SEP);
            if (nPath.endsWith(PathProcessor.WINDOWS_SEP)) {
                nPath = nPath.substring(0, nPath.lastIndexOf(PathProcessor.WINDOWS_SEP));
            }
        }
        return nPath;
    }

    SMBClient configureSMB() {
        // Set the timeout (optional)
        SmbConfig config = SmbConfig.builder().withTimeout(120, TimeUnit.SECONDS).withTimeout(120, TimeUnit.SECONDS)
                // Timeout sets read, write and Transact timeouts (default is 60 seconds)
                .withSoTimeout(180, TimeUnit.SECONDS) // Socket timeout (default is 0 seconds)
                .build();

        // If you do not set the timeout period SMBClient client = new SMBClient();
        SMBClient client = new SMBClient(config);
        return client;
    }

    @Override
    public BasicFile download(Drive drive, String path) throws IOException {
        String hostName = drive.getPropertyValue(HOST_NAME_PROPERTY_KEY);
        String sourceDriveName = drive.getPropertyValue(SHARE_NAME_PROPERTY_KEY); // e.g."SMB_Share");

        //SMBClient client = configureSMB();


        SMBClient client = new SMBClient();

        try (Connection connection = client.connect(hostName);
                Session session = connection.authenticate(getAuth(drive)))
        {
            DiskShare share = (DiskShare) session.connectShare(sourceDriveName);
            //                      FileIdBothDirectoryInformation destFile; // TODO FileAllInformation?
            //                      FileAllInformation destFile; // TODO FileAllInformation?

            String sourcePath = path;
            //                      String destPath = /* "C:\\Temp\\MDACA\\" + */ destFileName;

            // filePath should not include the share, e.g., it should be
            // "Temp\smallFile.txt"
            if (fileExists(share, sourcePath)) {
                java.io.File file = downloadFile(share, sourcePath);
                return new BasicFile(file.getAbsolutePath());
            } else {
                logger.info("Unable to download " + sourcePath + ".  File does not exist.");
            }
        } catch (Exception e) {
            logger.log(Level.WARNING, e.getMessage(), e);
        } finally {
            if (client != null) {
                client.close();
            }
        }
        return null;
    }

    @Override
    public void copy(Drive drive, String oldPath, String newPath) throws IOException {
        String hostName = drive.getPropertyValue(HOST_NAME_PROPERTY_KEY);
        String sourceDriveName = drive.getPropertyValue(SHARE_NAME_PROPERTY_KEY); // e.g."SMB_Share");
        String oldSmbPath = oldPath == null ? null
                : PathProcessor.convertToWindowsStylePath(oldPath);
        String newSmbPath = newPath == null ? null
                : PathProcessor.convertToWindowsStylePath(newPath);
        SMBClient client = configureSMB();

        try (Connection connection = client.connect(hostName);
                Session session = connection.authenticate(getAuth(drive)))
        {
            DiskShare share = (DiskShare) session.connectShare(sourceDriveName);
            // filePath should not include the share, e.g., it should be
            // "Temp\smallFile.txt"
            if (exists(share, oldSmbPath)) {

                if (exists(share, newSmbPath)) {
                    throw new FileAlreadyExistsException("Unable to copy " + oldSmbPath +
                            " to " + newSmbPath + ", because it already exists.");
                }
                try (InputStream stream = readBytes(share, oldSmbPath)) {
                    write(share, newSmbPath, stream);
                }
                logger.info("Copied " + oldSmbPath + " to " + newSmbPath);
            } else {
                throw new FileNotFoundException("Unable to copy " + oldSmbPath +
                        ".  File does not exist.");
            }
        } finally {
            if (client != null) {
                client.close();
            }
        }
    }

    void write(DiskShare share, final String path, InputStream is) throws IOException, SMBApiException {
        try (File file =
                share.openFile(path, EnumSet.of(AccessMask.GENERIC_WRITE), null, SMB2ShareAccess.ALL, SMB2CreateDisposition.FILE_CREATE, null)) {
            file.write(new InputStreamByteChunkProvider(is));
        }
    }

    private InputStream readBytes(DiskShare share, String remotePath) throws IOException, SMBApiException {
        //	    	see https://github.com/hierynomus/smbj/issues/174
        ByteArrayInputStream byteStream;
        try (File file = share.openFile(normalizePath(remotePath), EnumSet.of(AccessMask.GENERIC_READ), null, SMB2ShareAccess.ALL, SMB2CreateDisposition.FILE_OPEN, null)) {
            // TODO handle large files
            InputStream inputStream = file.getInputStream();
            byte[] bytes = IOUtils.toByteArray(inputStream);
            byteStream = new ByteArrayInputStream(bytes);
        }
        return byteStream;
    }

    @Override
    public void rename(Drive drive, String oldPath, String newPath) throws IOException {
        String hostName = drive.getPropertyValue(HOST_NAME_PROPERTY_KEY);
        String sourceDriveName = drive.getPropertyValue(SHARE_NAME_PROPERTY_KEY); // e.g."SMB_Share");
        String oldSmbPath = oldPath == null ? null
                : PathProcessor.convertToWindowsStylePath(oldPath);
        String newSmbPath = newPath == null ? null
                : PathProcessor.convertToWindowsStylePath(newPath);
        SMBClient client = configureSMB();
        DiskEntry entry = null;

        try (Connection connection = client.connect(hostName);
                Session session = connection.authenticate(getAuth(drive)))
        {
            DiskShare share = (DiskShare) session.connectShare(sourceDriveName);
            // filePath should not include the share, e.g., it should be
            // "Temp\smallFile.txt"
            if (exists(share, oldSmbPath)) {

                if (exists(share, newSmbPath)) {
                    throw new FileAlreadyExistsException("Unable to rename " + oldSmbPath +
                            " to " + newSmbPath + ", because it already exists.");
                }
                entry = share.open(oldSmbPath, EnumSet.of(AccessMask.DELETE, AccessMask.MAXIMUM_ALLOWED),
                        EnumSet.of(FileAttributes.FILE_ATTRIBUTE_NORMAL), SMB2ShareAccess.ALL,
                        SMB2CreateDisposition.FILE_OPEN, null);
                entry.rename(newSmbPath);
                entry.close();
                logger.info("Renamed " + oldSmbPath + " to " + newSmbPath);
            } else {
                throw new FileNotFoundException("Unable to rename " + oldSmbPath +
                        ".  File does not exist.");
            }
        } finally {
            if (client != null) {
                client.close();
            }
        }
    }

    public void renameWithProblem(Drive drive, String oldPath, String newPath) throws IOException {
        String hostName = drive.getPropertyValue(HOST_NAME_PROPERTY_KEY);
        String sourceDriveName = drive.getPropertyValue(SHARE_NAME_PROPERTY_KEY); // e.g."SMB_Share");
        SMBClient client = configureSMB();
        DiskEntry entry = null;

        try (Connection connection = client.connect(hostName);
                Session session = connection.authenticate(getAuth(drive)))
        {
            DiskShare share = (DiskShare) session.connectShare(sourceDriveName);
            // filePath should not include the share, e.g., it should be
            // "Temp\smallFile.txt"
            if (fileExists(share, oldPath)) {

                if (fileExists(share, newPath)) {
                    throw new IOException("Unable to rename " + oldPath +
                            " to " + newPath + ", because it already exists.");
                }
                entry = share.open(oldPath, EnumSet.of(AccessMask.DELETE, AccessMask.GENERIC_WRITE),
                        EnumSet.of(FileAttributes.FILE_ATTRIBUTE_NORMAL), SMB2ShareAccess.ALL,
                        SMB2CreateDisposition.FILE_OPEN, null);
                entry.rename(newPath);
                entry.close();
                logger.info("Renamed " + oldPath + " to " + newPath);
            } else {
                throw new IOException("Unable to rename " + oldPath +
                        ".  File does not exist.");
            }
        } finally {
            if (client != null) {
                client.close();
            }
        }
    }

    @Override
    public void deleteFile(Drive drive, String destPath) throws IOException {
        String hostName = drive.getPropertyValue(HOST_NAME_PROPERTY_KEY);
        String smbDriveName = drive.getPropertyValue(SHARE_NAME_PROPERTY_KEY); // e.g."SMB_Share");

        SMBClient client = configureSMB();
        try (Connection connection = client.connect(hostName);
                Session session = connection.authenticate(getAuth(drive))) {
            DiskShare share = (DiskShare) session.connectShare(smbDriveName);

            if (destPath.startsWith(PathProcessor.GUI_SEP)) {
                destPath = destPath.substring(1);
            }

            if (fileExists(share, destPath)) {
                share.rm(destPath);
                logger.info("Removed " + destPath);
            }
        }
        catch (Exception e) {
            logger.warning("Error while attempting to delete " + 
                    destPath + ": " + e.getMessage());
            throw e;
        }
    }

    @Override
    public void deleteDirectory(Drive drive, String destPath) throws IOException {
        String hostName = drive.getPropertyValue(HOST_NAME_PROPERTY_KEY);
        String smbDriveName = drive.getPropertyValue(SHARE_NAME_PROPERTY_KEY); // e.g."SMB_Share");

        SMBClient client = configureSMB();

        try (Connection connection = client.connect(hostName);
                Session session = connection.authenticate(getAuth(drive));
                DiskShare share = (DiskShare) session.connectShare(smbDriveName)) {

            if (destPath.startsWith(PathProcessor.GUI_SEP)) {
                destPath = destPath.substring(1);
            }

            if (folderExists(share, destPath)) {
                share.rmdir(destPath, true);
                logger.info("Removed " + destPath);
            }
        }
    }

    @Override
    public void mkdir(Drive drive, String sPath) throws IOException {
        if (exists(drive, sPath)) {
            logger.warning("Mkdir: " + sPath + " already exists.");
            throw new FileAlreadyExistsException("Attempted to create folder at path \"" + sPath + "\", but a folder already exists.");
        }

        SMBClient client = new SMBClient();
        String server = drive.getPropertyValue(HOST_NAME_PROPERTY_KEY); // "SSLW-01585"; // "192.168.1.233";
        String shareName = drive.getPropertyValue(SHARE_NAME_PROPERTY_KEY); // "SMB_Share";

        // TODO This is expensive.  Anything simpler? cache the connection
        try (Connection connection = client.connect(server);
                Session session = connection.authenticate(getAuth(drive));
                DiskShare share = (DiskShare) session.connectShare(shareName)) {
            ensureDirsExist(share, sPath);
            share.mkdir(removeFirstSlash(sPath));
        } catch (SMBApiException e) {
            String eMsg = "mkdir failed for " + removeFirstSlash(sPath) +
                    " - " + e.getMessage();
            logger.log(Level.WARNING, eMsg, e);
            throw new IOException(eMsg, e);
        } finally {
            client.close();
        }
    }

    @Override
    public boolean exists(Drive drive, String path) throws IOException {
        boolean doesExist = false;
        String hostName = drive.getPropertyValue(HOST_NAME_PROPERTY_KEY);
        String driveName = drive.getPropertyValue(SHARE_NAME_PROPERTY_KEY); // e.g."SMB_Share");
        SMBClient client = configureSMB();

        try (Connection connection = client.connect(hostName);
                Session session = connection.authenticate(getAuth(drive));
                DiskShare share = (DiskShare) session.connectShare(driveName)) {
            doesExist = exists(share, removeFirstSlash(path));
        }
        return doesExist;
    }

    Streamer fileWriter;

    public String uploadPartStart(Drive drive, String path) throws IOException {
        fileWriter = new Streamer(drive, path, true);
        return null;
    }

    @Override
    public void uploadPart(Drive drive, String path, byte[] data, int partNumber) throws IOException {
        fileWriter.getOutputStream().write(data);
    }

    public void uploadPartComplete(Drive drive, String path, String uploadId) throws IOException {
        fileWriter.getOutputStream().flush();
        fileWriter.close();
    }

    public void uploadPartAbort(Drive drive, String path, String uploadId) throws IOException {
        try {
            fileWriter.getDiskShare().rm(path);
        } catch(Exception ex)  {
            logger.log(Level.WARNING, ex.getMessage(), ex);
        }
        fileWriter.close();
    }

    public Streamer fileReader;

    public void downloadPartStart(Drive drive, String path) throws IOException {
        fileReader = new Streamer(drive, path, false);
    }

    public void downloadComplete(Drive drive, String path) throws IOException {
        if (fileReader != null) {
            fileReader.close();
        }
    }

    public byte[] downloadBytes(Drive drive, String path, long start, int numberOfBytes) throws IOException {
        byte[] bytes = null;
        long startTime = System.currentTimeMillis();
        if (numberOfBytes <= 0 || fileReader.isEmpty()) {
            return new byte[0];
        }

        byte[] data = new byte[numberOfBytes];
        int bytesRead = 0;
        int bufferLength = 1024;
        try (InputStream inputStream = fileReader.getInputStream(start)) {
            int read;
            do {
                if (numberOfBytes - bytesRead < bufferLength) {
                    bufferLength = (numberOfBytes - bytesRead);
                }

                read = inputStream.read(data, bytesRead, bufferLength);
                bytesRead += read;
            } while ((bytesRead < numberOfBytes) && (read == bufferLength));
            logger.info("Download of " + bytesRead + " bytes took " +
                    (System.currentTimeMillis() - startTime) + " ms.");
            bytes = (bytesRead == numberOfBytes) ? data : Arrays.copyOfRange(data, 0, bytesRead);
        }
        return bytes;
    }

    public InputStream getInputStream(Drive drive, String path) throws IOException {
        // TODO refactor - getting a InputStream like this has the potential for
        // leaving mutiple resources open
        if (fileReader == null) {
            fileReader = new Streamer(drive, path, false);
        }
        return fileReader.getInputStream(0);
    }

    /**
     * An extension of InputStream that will also close opened SMB resources
     */
    class SMBInputStream extends InputStream {

        Streamer streamer = null;

        SMBInputStream(Streamer streamer) {
            this.streamer = streamer;
        }

        @Override
        public void close() throws IOException {
            streamer.close();
        }

        ///////// The methods below merely delegate to the streamer's input stream

        @Override
        public int available() throws IOException {
            return streamer.inputStream.available();
        }

        @Override
        public int read() throws IOException {
            return streamer.inputStream.read();
        }

        @Override
        public int read(byte[] b) throws IOException {
            return streamer.inputStream.read(b);
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            return streamer.inputStream.read(b, off, len);
        }

        @Override
        public void mark(int i) {
            streamer.inputStream.mark(i);
        }

        @Override
        public boolean markSupported() {
            return streamer.inputStream.markSupported();
        }

        @Override
        public void reset() throws IOException {
            streamer.inputStream.reset();
        }

        @Override
        public long skip(long i) throws IOException {
            return streamer.inputStream.skip(i);
        }




    }

    class Streamer implements AutoCloseable {

        private final SMBClient client;
        private final Connection connection;
        private final Session session;
        private final DiskShare share;
        private final File file;
        private final OutputStream outputStream;
        private final InputStream inputStream;

        public Streamer(Drive drive, String path, boolean isUpload) throws IOException {
            String hostName = drive.getPropertyValue(HOST_NAME_PROPERTY_KEY);
            String driveName = drive.getPropertyValue(SHARE_NAME_PROPERTY_KEY); // e.g."SMB_Share");

            client = configureSMB();
            connection = client.connect(hostName);
            session = connection.authenticate(getAuth(drive));
            share = (DiskShare) session.connectShare(driveName);
            if (isUpload) {
                // When a list of files comes from a cloud storage provider,
                // the intermediate "directories" may not exist.  We create
                // directories corresponding to the prefixes, if necessary.
                ensureDirsExist(share, path);
                file = share.openFile(path, EnumSet.of(AccessMask.GENERIC_ALL),
                        EnumSet.of(FileAttributes.FILE_ATTRIBUTE_NORMAL), SMB2ShareAccess.ALL,
                        SMB2CreateDisposition.FILE_CREATE, EnumSet.of(SMB2CreateOptions.FILE_DIRECTORY_FILE));
            }
            else { // download
                file = share.openFile(path, EnumSet.of(AccessMask.GENERIC_READ), null, SMB2ShareAccess.ALL,
                        SMB2CreateDisposition.FILE_OPEN, null);
            }
            outputStream = file.getOutputStream();
            inputStream = file.getInputStream();
        }

        private OutputStream getOutputStream() {
            return outputStream;
        }

        private InputStream getInputStream(long skip) throws IOException {
            InputStream inputStream = file.getInputStream();
            inputStream.skip(skip);
            return inputStream;
        }

        private boolean isEmpty() throws IOException {
            return getFile().getInputStream().read() == -1;
        }

        private com.hierynomus.smbj.share.File getFile() {
            return file;
        }

        private DiskShare getDiskShare() {
            return share;
        }

        public void close() throws IOException {
            if (inputStream != null) {
                inputStream.close();
            }
            if (outputStream != null) {
                outputStream.close();
            }
            if (file != null) {
                file.close();
            }
            if (share != null) {
                share.close();
            }
            if (session != null) {
                session.close();
            }
            if (connection != null) {
                connection.close();
            }
            if (client != null) {
                client.close();
            }
        }

    }

    @Override
    public List<String> getProperties() {
        return Arrays.asList(
                HOST_NAME_PROPERTY_KEY,
                DOMAIN_PROPERTY_KEY,
                USER_NAME_PROPERTY_KEY,
                PASSWORD_PROPERTY_KEY,
                SHARE_NAME_PROPERTY_KEY);
    }

}
