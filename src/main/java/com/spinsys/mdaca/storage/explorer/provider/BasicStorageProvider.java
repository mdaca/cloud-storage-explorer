package com.spinsys.mdaca.storage.explorer.provider;

import com.spinsys.mdaca.storage.explorer.io.BasicFile;
import com.spinsys.mdaca.storage.explorer.io.FileUtil;
import com.spinsys.mdaca.storage.explorer.io.PathProcessor;
import com.spinsys.mdaca.storage.explorer.model.DriveItem;
import com.spinsys.mdaca.storage.explorer.model.DriveQuery;
import com.spinsys.mdaca.storage.explorer.model.dto.DriveMemoryUsageDTO;
import com.spinsys.mdaca.storage.explorer.model.enumeration.DriveType;
import com.spinsys.mdaca.storage.explorer.model.exception.ExplorerException;
import com.spinsys.mdaca.storage.explorer.persistence.Drive;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.spinsys.mdaca.storage.explorer.io.PathProcessor.UNIX_SEP;
import static com.spinsys.mdaca.storage.explorer.io.PathProcessor.WINDOWS_SEP;

/**
 * This class manipulates files and directories on a
 * mounted file system using basic Java I/O.
 *
 * @author Keith Cassell
 */
// TODO remove some of the more obvious Windows-orientation
public class BasicStorageProvider extends AbstractStorageProvider<File> {

    //BASIC storage provider - this is the drive letter
    public static final String DRIVE_NAME_PROPERTY_KEY = "DriveName";

    private static final Logger logger =
            Logger.getLogger("com.spinsys.mdaca.storage.explorer.provider.BasicStorageProvider");

    /** The separator used by this provider */
    protected String sep = UNIX_SEP;

    /** The output file to use when transferring file contents
     *  in chunks. */
    File chunkFile = null;

    /** The output stream to use when transferring file contents
     *  in chunks. */
    FileOutputStream chunkOutputStream = null;

    public BasicStorageProvider() {
    }

    @Override
    public List<DriveItem> find(Drive drive, DriveQuery query) throws ExplorerException {
        String startPath = query.getStartPath();
        String folder = getPathWithDriveLetter(drive, (startPath == null) ?
                PathProcessor.getParentFolder(query.getSearchPattern())
                : startPath);

        BasicFile start = new BasicFile(folder);
        List<File> foundFiles = start.list(query);
        logger.info("Found " + foundFiles.size() + " matching query " + query);

        return buildAndFilterDriveItems(foundFiles, drive, query);
    }

    @Override
    public DriveItem buildDriveItem(File file, Drive drive, Object...metadata) {
        DriveItem item = new DriveItem();
        item.setDriveId(drive.getDriveId());
        item.setFileSize(file.length());
        Date date = new Date(file.lastModified());
        item.setModifiedDate(date);

        Path path = file.toPath();
        String sPath = PathProcessor.convertToUnixStylePath(path.toString());
        sPath = PathProcessor.removeDriveLetter(sPath);
        
        boolean isDir = Files.isDirectory(path);
        item.setDirectory(isDir);
        
        if (isDir) {
            item.setPath(sPath + "/");
        }
        else {
            item.setPath(sPath);
        }
        return item;
    }

    @Override
    public DriveItem getDriveItem(Drive drive, String path) throws ExplorerException {
        path = getPathWithDriveLetter(drive, path);
        File file = new File(path);

        return buildDriveItem(file, drive);
    }

    @Override
    public BasicFile download(Drive drive, String sPathIn) throws IOException {
        BasicFile tempFile = null;

        if (sPathIn != null && exists(drive, sPathIn)) {
            String sPath = getPathWithDriveLetter(drive, sPathIn);

            // TODO special handling for directories?
            tempFile = FileUtil.buildTempFile("BSP_download");
            FileOutputStream fos = new FileOutputStream(tempFile);
            Path path = Paths.get(sPath);
            Files.copy(path, fos);
            logger.info("Download from " + sPath + " to " + tempFile.getAbsolutePath() + " succeeded");
        } else {
            logger.info("Unable to locate file to download: " + sPathIn);
        }
        return tempFile;
    }

    @Override
    public void upload(Drive drive, String sDestPathIn, File inputFile) throws IOException {
        if (sDestPathIn == null || sDestPathIn.isEmpty()) {
            throw new IOException("No destination file specified.");
        }

        if (inputFile == null) {
            throw new IOException("No source file specified.");
        }

        String destPath = getPathWithDriveLetter(drive, sDestPathIn);
        File destFile = new File(destPath);

        if (exists(drive, destPath)) {
            throw new FileAlreadyExistsException("Attempted to upload \"" + destPath + "\", but it already exists");
        }

        ensureDirsExist(drive, destPath); // may create a dir at destPath
        FileUtils.copyFile(inputFile, destFile);
        logger.info("Upload of " + inputFile.toPath() + " to " + destPath + " complete.");
    }

    @Override
    public void copy(Drive drive, String oldName, String newName) throws IOException {
        if (!exists(drive, newName)) {
            File oldFile = new File(oldName);
            File newFile = new File(newName);

            if (oldFile.isDirectory()) {
                FileUtils.copyDirectory(oldFile, newFile);
            } else {
                FileUtils.copyFile(oldFile, newFile);
            }
        } else {
            throw new FileAlreadyExistsException(newName + " already exists.");
        }
    }

    @Override
    public void rename(Drive drive, String oldName, String newName) throws IOException {
        File oldFile = new File(oldName);
        File newFile = new File(newName);

        if (oldFile.isDirectory()) {
            FileUtils.moveDirectory(oldFile, newFile);
        } else {
            FileUtils.moveFile(oldFile, newFile);
        }
    }

    @Override
    public void deleteFile(Drive drive, String destPath) throws IOException {
        Path path = Paths.get(destPath);
        Files.delete(path);
    }

    @Override
    public void deleteDirectory(Drive drive, String path) throws IOException {
        path = getPathWithDriveLetter(drive, path);
        BasicFile basicFile = new BasicFile(path);

        basicFile.deleteDirectory();
    }

    @Override
    public void mkdir(Drive drive, String sPath) throws IOException {
        if (sPath != null) {
            sPath = getPathWithDriveLetter(drive, sPath);
            Path path = new File(sPath).toPath();
            Files.createDirectory(path);
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
    public boolean testConnection(Drive drive) throws ExplorerException {
        boolean exists = false;
        String sPath = getPathWithDriveLetter(drive, PathProcessor.GUI_SEP);

        if (sPath != null) {
            Path path = Paths.get(sPath);
            // If we can confirm the root directory exists, we
            // have a connection
            exists = Files.exists(path);
        }
        try {
            exists = exists(drive, PathProcessor.GUI_SEP);
        }
        catch (Exception e) {
            throw new ExplorerException("Exception while testing connection to " +
                    drive + ". " + e.getMessage(), e);
        }
        return exists;
    }

    /**
     * Make sure the dirs containing the file exist,
     * creating them if necessary
     * @param drive
     * @param destPath
     * @throws IOException
     */
    protected void ensureDirsExist(Drive drive, String destPath) throws IOException {
        String parentPath = PathProcessor.getParentFolderPath(destPath);

        while (!PathProcessor.isRoot(parentPath)) {
            if (!exists(drive, parentPath)) {
                mkdir(drive, parentPath);
            }

            parentPath = PathProcessor.getParentFolderPath(parentPath);
        }
    }

    @Override
    public boolean isDirectory(Drive drive, String sPathIn) throws IOException {
        boolean isDir = false;
        String sPath = getPathWithDriveLetter(drive, sPathIn);
        if (sPath != null) {
            Path path = Paths.get(sPath);
            isDir = Files.isDirectory(path);
        }
        return isDir;
    }

    @Override
    public String normalizePath(String sPath) {
        String result = null;

        if (sPath != null) {
            Path path = Paths.get(sPath);
            result = path.normalize().toString();
        }
        return result;
    }

    public String getPathWithDriveLetter(Drive drive, String path) {
        //skip if this isn't a windows provider OR the drive letter is already applied
        if (!DriveType.Windows.equals(drive.getDriveType()) ||
                PathProcessor.startsWithDriveLetter(path)) {
            return path;
        }
        path = (path == null) ? "" : normalizePath(path);
        //remove the first slash
        path = path.startsWith(WINDOWS_SEP) ? path.substring(1) : path;

        String driveLetter = drive.getPropertyValue(DRIVE_NAME_PROPERTY_KEY);

        return (driveLetter + ":" + WINDOWS_SEP + path);
    }

    @Override
    public byte[] downloadBytes(Drive drive, String sPathIn, long startByte, int numberOfBytes) throws IOException {
        byte[] bytes = new byte[numberOfBytes];

        if (sPathIn != null && exists(drive, sPathIn)) {
            String sPath = getPathWithDriveLetter(drive, sPathIn);
            long start = System.currentTimeMillis();
            try (RandomAccessFile file = new RandomAccessFile(sPath, "r")) {
                file.seek(startByte);
                //				long preSeek = System.currentTimeMillis();
                //				logger.info("Seek took " +
                //						(System.currentTimeMillis() - preSeek) + " ms.");
                int numRead = file.read(bytes, 0, numberOfBytes);

                if (numRead < numberOfBytes) { // only return the bytes actually read
                    if (numRead < 0) {
                        bytes = new byte[0];
                    }
                    else {
                        bytes = Arrays.copyOfRange(bytes, 0, numRead);
                    }
                }
                logger.info("Download of " + numRead + " bytes from " +
                        sPath + " took " +
                        (System.currentTimeMillis() - start) + " ms.");
            }
        } else {
            String msg = "Unable to read bytes from " + sPathIn;
            logger.info(msg);
            throw new IOException(msg);
        }
        return bytes;
    }

    @Override
    public String uploadPartStart(Drive drive, String sDestPathIn) throws IOException {
        if (sDestPathIn == null || sDestPathIn.isEmpty()) {
            throw new IOException("No destination file specified.");
        }
        String sDestPath = getPathWithDriveLetter(drive, sDestPathIn);
        ensureDirsExist(drive, sDestPath);
        chunkFile = new File(sDestPath);
        chunkOutputStream = new FileOutputStream(chunkFile);
        logger.info("Starting to upload chunks to " + sDestPath);
        return sDestPath;
    }

    @Override
    public void uploadPart(Drive drive, String path, byte[] data, int partNumber) throws IOException {
        long start = System.currentTimeMillis();
        chunkOutputStream.write(data);
        logger.info("Upload of " + data.length + " bytes took " +
                (System.currentTimeMillis() - start ) + " ms.");
    }

    @Override
    public void uploadPartComplete(Drive drive, String path, String uploadId) throws IOException {
        try {
            logger.info("Upload to " + path + " complete.");
            if (chunkOutputStream != null) {
                chunkOutputStream.flush();
                chunkOutputStream.close();
                chunkOutputStream = null;
            }
            chunkFile = null;
        } catch (IOException e) {
            logger.log(Level.WARNING, e.getMessage(), e);
        } finally {
            try {
                if (chunkOutputStream != null) {
                    chunkOutputStream.close();
                    chunkOutputStream = null;
                }
                if (chunkFile != null) {
                    chunkFile = null;
                }
            } catch (IOException e) {
                logger.log(Level.WARNING, e.getMessage(), e);
            }
        }
    }

    @Override
    public void uploadPartAbort(Drive drive, String path, String uploadId) throws IOException {
        try {
            logger.info("Upload to " + path + " aborted.");
            chunkOutputStream.flush();
            chunkOutputStream.close();
            chunkOutputStream = null;
            chunkFile.delete();
            chunkFile = null;
        } catch (IOException e) {
            logger.log(Level.WARNING, e.getMessage(), e);
        } finally {
            try {
                if (chunkOutputStream != null) {
                    chunkOutputStream.close();
                    chunkOutputStream = null;
                }
                if (chunkFile != null) {
                    chunkFile.delete();
                    chunkFile = null;
                }
            } catch (IOException e) {
                logger.log(Level.WARNING, e.getMessage(), e);
            }
        }
    }

    public InputStream getInputStream(Drive drive, String sPathIn) throws IOException {
        InputStream stream = null;
        if (sPathIn != null && exists(drive, sPathIn)) {
            String sPath = getPathWithDriveLetter(drive, sPathIn);
            File initialFile = new File(sPath);
            stream = new FileInputStream(initialFile);
        } else {
            logger.info("Unable to locate file: " + sPathIn);
        }
        return stream;
    }

    @Override
    public String getHiveLocationPath(Drive sourceDrive, String sourcePath) {
        return null;
    }

    @Override
    public List<String> getProperties() {
        return Collections.singletonList(DRIVE_NAME_PROPERTY_KEY);
    }

}
