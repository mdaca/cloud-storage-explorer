package com.spinsys.mdaca.storage.explorer.io;

import com.spinsys.mdaca.storage.explorer.model.DriveQuery;
import net.lingala.zip4j.ZipFile;
import net.lingala.zip4j.exception.ZipException;
import org.apache.commons.compress.compressors.z.ZCompressorInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.rauschig.jarchivelib.ArchiveFormat;
import org.rauschig.jarchivelib.Archiver;
import org.rauschig.jarchivelib.ArchiverFactory;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

public class BasicFile extends File {

	private static final long serialVersionUID = -8173960431581919426L;

	public static final Logger logger =
			Logger.getLogger("com.spinsys.mdaca.storage.explorer.io.BasicFile");


	public BasicFile(String pathname) {
        super(pathname);
    }

    public BasicFile(URI uri) {
        super(uri);
    }

    /**
     * @param newPath
     * @return the untarred file
     * @throws IOException
     */
    public File untarFile(String newPath) throws IOException {
        if (!exists()) {
            throw new FileNotFoundException("Attempted to untar file \"" + getName() + "\", but it was not found");
        }

        Archiver archiver = ArchiverFactory.createArchiver(ArchiveFormat.TAR);
        BasicFile untarredRoot = new BasicFile(newPath);
		archiver.extract(getCanonicalFile(), untarredRoot);
        return untarredRoot;
    }

    /**
     * @param newFileTempPath
     * @return the unZipped file
     */
    public BasicFile unZipFile(String newFileTempPath) throws ZipException, FileNotFoundException {
        if (!exists()) {
            throw new FileNotFoundException("Attempted to unzip file \"" + getName() + "\", but it was not found");
        }

        ZipFile zipFile = new ZipFile(this);

        //unzip the directory
        try {
        	logger.info("Extracting " + zipFile.getFile() +
        				" to " + newFileTempPath + ".");
        	zipFile.extractAll(newFileTempPath);
        }
        catch (ZipException e) {
        	logger.warning("Failed to extract " + zipFile.getFile() +
        					" to " + newFileTempPath + ".  " + e.getMessage());
        	throw e;
        }
        return new BasicFile(newFileTempPath);
    }

    /**
     * @param newFilePath the path of the decompressed file being created
     * @return the unGzipped file
     */
    public BasicFile unGzipFile(String newFilePath) throws IOException {
        try(FileInputStream fis = new FileInputStream(getAbsolutePath());
	        GZIPInputStream gis = new GZIPInputStream(fis);
	        FileOutputStream fos = new FileOutputStream(newFilePath)) {
	        byte[] buffer = new byte[1024];
	        int len;
	        while ((len = gis.read(buffer)) != -1) {
	            fos.write(buffer, 0, len);
	        }
	        //close resources
	        fos.close();
	        gis.close();

	        return new BasicFile(newFilePath);
        }
    }

    /**
     * @param newFilePath the path of the decompressed file being created
     * @return the unGzipped file
     */
    public BasicFile uncompressZArchive(String newFilePath) throws IOException {
        try (FileInputStream fis = new FileInputStream(getAbsolutePath());
        		BufferedInputStream bufIn = new BufferedInputStream(fis);
        		OutputStream out = Files.newOutputStream(Paths.get(newFilePath));
        		ZCompressorInputStream zIn = new ZCompressorInputStream(bufIn)) {
	        byte[] buffer = new byte[1024];
	        int len;
	        while ((len = zIn.read(buffer)) != -1) {
	            out.write(buffer, 0, len);
	        }
        }
        return new BasicFile(newFilePath);
    }

    public String contents() throws IOException {
    	String fileContents = "";
        Path path = Paths.get(getAbsolutePath());

        List<String> lines = Files.readAllLines(path);

        if (lines.size() > 0) {
    		fileContents = lines.get(0);
        }
        return fileContents;
    }

    public void write(String contents) throws IOException {
        FileWriter fileWriter = new FileWriter(this);
        fileWriter.write(contents);
        fileWriter.close();
    }

    public void deleteDirectory() throws IOException {
        FileUtils.deleteDirectory(this);
    }

    /**
     * folders in some cloud services are not allowed to be empty
     * below is the name of a dummy file that will be placed to keep the folder from being deleted
     * this file will be skipped when collecting the names inside depending on the DriveQuery
     * DriveQuery.setUsesPlaceholder(false) -> do NOT include the placeholder file in the search
     */
    //TODO need to delete this eventually if not used
    public static BasicFile buildTempFile(String name) throws IOException {
        File placeholderFile = File.createTempFile(name, "");
        BasicFile basicFile = FileUtil.getTempBasicFile(placeholderFile);
        return basicFile;
    }

    @Override
    public boolean delete() {
        boolean wasDeleted = super.delete();

        if (!wasDeleted) {
            logger.info("Unable to delete file - " + getName());
        }

        return wasDeleted;
    }

    /**
     * @param query the filters to limit the returned drive item list
     * @return a list of the files AND directories at this level
     */
    public List<File> list(DriveQuery query) {
        String stringRegexFilter = query.hasSearchPattern() ? query.getSearchPattern() : ".*";
        RegexFileFilter regexFilter = new RegexFileFilter(stringRegexFilter);

        List<File> filesAndDirs;
        if (query.isRecursive()) {
            Collection<File> filesCollection = FileUtils.listFilesAndDirs(this, regexFilter, DirectoryFileFilter.DIRECTORY);
            filesAndDirs = new ArrayList<>(filesCollection);
        } else {
            //this.listFiles DOES return all files AND dirs
            File[] files = this.listFiles((FileFilter) regexFilter);

            filesAndDirs = (files != null) ? Arrays.asList(files) : new ArrayList<>();
        }

        return filesAndDirs;
    }

}
