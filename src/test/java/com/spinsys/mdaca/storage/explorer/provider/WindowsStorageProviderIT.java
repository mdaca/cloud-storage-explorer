package com.spinsys.mdaca.storage.explorer.provider;

import static com.spinsys.mdaca.storage.explorer.provider.BasicStorageProvider.DRIVE_NAME_PROPERTY_KEY;
import static com.spinsys.mdaca.storage.explorer.provider.SMBStorageProvider.DOMAIN_PROPERTY_KEY;
import static com.spinsys.mdaca.storage.explorer.provider.SMBStorageProvider.HOST_NAME_PROPERTY_KEY;
import static com.spinsys.mdaca.storage.explorer.provider.SMBStorageProvider.PASSWORD_PROPERTY_KEY;
import static com.spinsys.mdaca.storage.explorer.provider.SMBStorageProvider.SHARE_NAME_PROPERTY_KEY;
import static com.spinsys.mdaca.storage.explorer.provider.SMBStorageProvider.USER_NAME_PROPERTY_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.util.Date;
import java.util.List;
import java.util.logging.Logger;

import com.spinsys.mdaca.storage.explorer.io.PathProcessor;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import com.spinsys.mdaca.storage.explorer.io.BasicFile;
import com.spinsys.mdaca.storage.explorer.io.FileUtil;
import com.spinsys.mdaca.storage.explorer.model.DriveItem;
import com.spinsys.mdaca.storage.explorer.model.DriveQuery;
import com.spinsys.mdaca.storage.explorer.model.enumeration.DriveType;
import com.spinsys.mdaca.storage.explorer.model.exception.ExplorerException;
import com.spinsys.mdaca.storage.explorer.persistence.Drive;
import com.spinsys.mdaca.storage.explorer.persistence.DriveProperty;

/**
 * Contains integration tests for WindowsStorageProvider
 * 
 * These tests are highly dependent on files and directories
 * being in certain places on the drive, on certain environment
 * variables (see initializeTestDrive()), and on test ordering
 * (@Order).
 */
// TODO - reduce dependencies, further automate initial state restoration

@TestMethodOrder(OrderAnnotation.class)
public class WindowsStorageProviderIT extends AbstractProviderIT {
	
	/** A name for a directory that should already exist. */
	private static final String PREEXISTING_DIR = "csv-dir";

	/** A file name for a file that should already exist. */
	private static final String PREEXISTING_FILE = "leaveMeAlone.txt";

	/** A file name for a file that should already exist in a subdirectory. */
	private static final String PREEXISTING_SUBDIR_FILE =
			PREEXISTING_DIR + "\\readme.txt";

	/** A file name for a file that should already exist in a subdirectory,
	 * using a "/" for the file separator. */
	private static final String PREEXISTING_SUBDIR_FILE_SLASH =
			PREEXISTING_DIR + "/readme.txt";

    private static final Logger logger =
            Logger.getLogger("com.spinsys.mdaca.storage.explorer.provider.WindowsStorageProviderIT");

	/** We'll store this file between tests, so we can rename it,
	 * delete it, etc.  If we're lucky, we can sequence our tests
	 * such that the Windows drive ends up in the same state it was
	 * in the beginning by deleting this uploaded file at the end.
	 */
	private static String uploadedFileName = null;

	public WindowsStorageProviderIT() {
		provider = new WindowsStorageProvider();
	}
	

	public Drive buildDrive() {
		Drive drive = new Drive();
		drive.setDisplayName("Windows_Share");
		drive.setDriveType(DriveType.Windows);
		drive.setDriveId(2);
		drive.addPropertyValue(USER_NAME_PROPERTY_KEY,
				System.getenv(USER_NAME_PROPERTY_KEY));
		drive.addPropertyValue(PASSWORD_PROPERTY_KEY,
				System.getenv(PASSWORD_PROPERTY_KEY));
		drive.addPropertyValue(DOMAIN_PROPERTY_KEY, null);
		drive.addPropertyValue(HOST_NAME_PROPERTY_KEY,
				System.getenv(HOST_NAME_PROPERTY_KEY));
		drive.addPropertyValue(SHARE_NAME_PROPERTY_KEY,
				System.getenv(SHARE_NAME_PROPERTY_KEY));
		String driveName = System.getenv(DRIVE_NAME_PROPERTY_KEY);
		drive.addPropertyValue(DRIVE_NAME_PROPERTY_KEY,
				(driveName == null || driveName.isEmpty() ? "Z" : driveName));
		return drive;
	}

    public Drive buildCDrive() {
        Drive drive = new Drive();
        drive.setDisplayName("C Drive");
        drive.setDriveType(DriveType.Windows);
        drive.setDriveId(2);
        drive.addPropertyValue(USER_NAME_PROPERTY_KEY,
                System.getenv(USER_NAME_PROPERTY_KEY));
        drive.addPropertyValue(PASSWORD_PROPERTY_KEY,
                System.getenv(PASSWORD_PROPERTY_KEY));
        drive.addPropertyValue(DOMAIN_PROPERTY_KEY, null);
        drive.addPropertyValue(HOST_NAME_PROPERTY_KEY,
                System.getenv(HOST_NAME_PROPERTY_KEY));
        drive.addPropertyValue(SHARE_NAME_PROPERTY_KEY,
                System.getenv(SHARE_NAME_PROPERTY_KEY));
        drive.addPropertyValue(DRIVE_NAME_PROPERTY_KEY, "C");
        return drive;
    }

	@Test
	@Tag("integration")
	@Order(10)
	public void testExistsFile() throws IOException {
		Drive drive = buildDrive();

		assertTrue(provider.exists(drive, PREEXISTING_FILE),
				PREEXISTING_FILE + " should exist on " + drive);
	}

	@Test
	@Tag("integration")
	@Order(11)
	public void testExistsDir() throws IOException {
		Drive drive = buildDrive();

		assertTrue(provider.exists(drive, PREEXISTING_DIR),
				PREEXISTING_DIR + " should exist on " + drive);
	}

	@Test
	@Tag("integration")
	@Order(12)
	public void testExistsSubdirFile() throws IOException {
		Drive drive = buildDrive();

		assertTrue(provider.exists(drive, PREEXISTING_SUBDIR_FILE),
				PREEXISTING_SUBDIR_FILE + " should exist on " + drive);
	}

	@Test
	@Tag("integration")
	@Order(13)
	public void testExistsFalse() throws IOException {
		Drive drive = buildDrive();

		String path = "ybgfuyc76";
		assertFalse(provider.exists(drive, path),
				path + " should not exist on " + drive);
	}

	@Test
	@Tag("integration")
	public void testDownloadFileNormal() throws IOException {
		Drive drive = buildDrive();

		File download = provider.download(drive, PREEXISTING_FILE);
		assertTrue(provider.exists(drive, download.getAbsolutePath()),
				PREEXISTING_FILE + " should have downloaded");
		// clean up
		download.delete();
	}

	@Test
	@Tag("integration")
	public void testDownloadFileMissing() throws IOException {
		Drive drive = buildDrive();

		DriveQuery query = new DriveQuery();
		query.setDriveId(drive.getDriveId());
		String path = "ifyufqvf";
		File download = provider.download(drive, path);
		assertNull(download, "Attempt to download non-existent " + path +
				" should have returned null.");
	}

	/**
	 * Testing an attempt to find a file whose name contains
	 * nonalphanumic characters that are treated specially
	 * within regular expressions.
	 * @throws IOException
	 */
	@Test
	@Tag("integration")
	public void testFindNonAlphanumeric() throws IOException {
		String fileNameTofind = "fileOne (1) (2).txt";
		File inputFile = FileUtil.getResourceFile("sampleFolder/" + fileNameTofind);

		String path = PathProcessor.removeDriveLetter(inputFile.getCanonicalPath());

		DriveItem driveItem = provider.getDriveItem(drive, path);
		assertEquals(fileNameTofind, driveItem.getFileName());
	}

	/**
	 * Testing an attempt to find a file that doesn't exist.
	 * @throws IOException
	 */
	@Test
	@Tag("integration")
	public void testFindGarbageRecursive() throws IOException {
		Drive drive = buildDrive();
		DriveQuery query = new DriveQuery();
		query.setDriveId(drive.getDriveId());
		query.setRecursive(true);
		query.setSearchPattern(".*yurwufru.*");
		query.setStartPath(PREEXISTING_DIR); // start at csv-dir
		
		List<DriveItem> items = provider.find(drive, query);
		int size = items.size();
		logger.info("Found " + size + " results using the query: " + query);
		assertEquals(0, size, "There should be no disk objects matching .*yurwufru.*");
	}


	@Test
	@Tag("integration")
	@Order(22)
	public void testFindFromRootUsingRegex() throws IOException {
		Drive drive = buildDrive();

		DriveQuery query = new DriveQuery();
		query.setDriveId(drive.getDriveId());
		query.setRecursive(true);
		query.setSearchPattern(".*txt.*");

		List<DriveItem> items = provider.find(drive, query);
		boolean txtMatch = items.stream().anyMatch(di -> di.getPath().contains("txt"));
		assertTrue(txtMatch);
	}

	@Test
	@Tag("integration")
	@Order(23)
	public void testFindAll() throws IOException {
		Drive drive = buildDrive();

		DriveQuery query = new DriveQuery();
		query.setDriveId(drive.getDriveId());
		query.setRecursive(true);
		query.setSearchPattern(""); // get everything
//			query.setStartPath(PREEXISTING_DIR); // start at csv-dir
		
		List<DriveItem> items = provider.find(drive, query);
		int size = items.size();
		logger.info("Found " + size + " results using the query: " + query);
		assertTrue(size > 0);

		query.setSearchPattern(".*txt.*");
		query.setStartPath(PREEXISTING_DIR); // start at csv-dir
		
		List<DriveItem> itemsK = provider.find(drive, query);
		int sizeK = itemsK.size();
		logger.info("Found " + sizeK + " results using the query: " + query);
		assertTrue(sizeK > 0);
		boolean txtMatch = items.stream().anyMatch(di -> di.getPath().contains("txt"));
		assertTrue(txtMatch);
		assertTrue(sizeK < size);
	}

	@Test
	@Tag("integration")
	@Order(24)
	public void testFromDir() throws IOException {
		Drive drive = buildDrive();

		DriveQuery query = new DriveQuery();
		query.setDriveId(drive.getDriveId());
		query.setRecursive(true);
		query.setSearchPattern(""); // get everything
		
		List<DriveItem> items = provider.find(drive, query);
		int size = items.size();
		logger.info("Found " + size + " results using the query: " + query);
		assertTrue(size > 0);

		query.setStartPath(PREEXISTING_DIR); // start at csv-dir		
		List<DriveItem> itemsCSV = provider.find(drive, query);
		int sizeCSV = itemsCSV.size();
		logger.info("Found " + sizeCSV + " results using the query: " + query);
		assertTrue(sizeCSV > 0);
		assertTrue(itemsCSV.get(0).getPath().contains("csv"));
		assertTrue(sizeCSV < size);
	}

	@Test
	@Tag("integration")
	@Order(50)
	public void testUpload() throws IOException {
		Drive drive = buildDrive();
		
		File sampleFile = createSampleFile();
		String fileName = sampleFile.getName();
		provider.upload(drive, fileName, sampleFile);

		assertTrue(provider.exists(drive, fileName));

		provider.delete(drive, fileName);
		assertFalse(provider.exists(drive, fileName));
	}

	@Test
	@Tag("integration")
	@Order(51)
	public void testUploadExistingThrowsException() throws IOException {
		Drive drive = buildDrive();

		File sampleFile = createSampleFile();
		String fileName = sampleFile.getName();
		provider.upload(drive, fileName, sampleFile);

		assertTrue(provider.exists(drive, fileName));

		assertThrows(FileAlreadyExistsException.class, () ->
				provider.upload(drive, fileName, sampleFile)
		);

		provider.delete(drive, fileName);
		assertFalse(provider.exists(drive, fileName));
	}

	@Test
	public void testUploadDirectory() throws IOException {
		String dirName = "src/test/resources/quotesFolder";
		File dir = new File(dirName);

		Drive drive = buildDrive();
		String folderName = "UploadedQuotesFolder";
		provider.uploadDirectory(drive, folderName, dir);

		DriveQuery query = new DriveQuery("UploadedQuotesFolder");
		query.setRecursive(true);
		List<DriveItem> driveItems = provider.find(drive, query);
		assertEquals(3, driveItems.size());
		
		provider.deleteDirectory(drive, folderName);
		assertFalse(provider.exists(drive, folderName),
					folderName + " should be deleted, but still exists");
	}

	@Test
	@Tag("integration")
	@Order(60)
	public void testRename() throws IOException {
		Drive drive = buildDrive();

		File sampleFile = createSampleFile();
		String fileName = sampleFile.getName();
		provider.upload(drive, fileName, sampleFile);

		String mvFileName = fileName + "mv";
		provider.rename(drive, fileName, mvFileName);
		assertFalse(provider.exists(drive, fileName),
				fileName + " was not renamed");

		assertTrue(provider.exists(drive, mvFileName),
					mvFileName + " not found");
		logger.info("Found " + mvFileName + " after rename.");

		Assertions.assertThrows(IOException.class, () ->
				provider.rename(drive, PREEXISTING_FILE, mvFileName)
		);
	
		// Update the stored file name so we can delete it later
		uploadedFileName = mvFileName;
	}

	@Test
	@Tag("integration")
	@Order(61)
	public void testRenameSubdirFile() throws IOException {
		Drive drive = buildDrive();

		assertTrue(provider.exists(drive, PREEXISTING_SUBDIR_FILE),
				PREEXISTING_SUBDIR_FILE + " not found");
		logger.info("Found " + PREEXISTING_SUBDIR_FILE + " before rename.");

		String mvFileName = PREEXISTING_SUBDIR_FILE + "mv";
		provider.rename(drive, PREEXISTING_SUBDIR_FILE, mvFileName);
		assertFalse(provider.exists(drive, PREEXISTING_SUBDIR_FILE),
				PREEXISTING_SUBDIR_FILE + " was not renamed");

		assertTrue(provider.exists(drive, mvFileName),
					mvFileName + " not found");
		logger.info("Found " + mvFileName + " after rename.");
		
		// Restore the original state
		provider.rename(drive, mvFileName, PREEXISTING_SUBDIR_FILE);
		assertTrue(provider.exists(drive, PREEXISTING_SUBDIR_FILE),
				PREEXISTING_SUBDIR_FILE + " not found");
	}

	@Test
	@Tag("integration")
	@Order(61)
	public void testRenameSubdirFileSlash() throws IOException {
		Drive drive = buildDrive();

		assertTrue(provider.exists(drive, PREEXISTING_SUBDIR_FILE_SLASH),
				PREEXISTING_SUBDIR_FILE_SLASH + " not found");
		logger.info("Found " + PREEXISTING_SUBDIR_FILE_SLASH + " before rename.");

		String mvFileName = PREEXISTING_SUBDIR_FILE_SLASH + "mv";
		provider.rename(drive, PREEXISTING_SUBDIR_FILE_SLASH, mvFileName);
		assertFalse(provider.exists(drive, PREEXISTING_SUBDIR_FILE_SLASH),
				PREEXISTING_SUBDIR_FILE_SLASH + " was not renamed");

		assertTrue(provider.exists(drive, mvFileName),
					mvFileName + " not found");
		logger.info("Found " + mvFileName + " after rename.");
		
		// Restore the original state
		provider.rename(drive, mvFileName, PREEXISTING_SUBDIR_FILE_SLASH);
		assertTrue(provider.exists(drive, PREEXISTING_SUBDIR_FILE_SLASH),
				PREEXISTING_SUBDIR_FILE_SLASH + " not found");
	}

	/**
	 * Typical case - new directory, no name conflict
	 * @throws IOException
	 */
	@Test
	@Tag("integration")
	@Order(81)
	public void testMkdir() throws IOException {
		Drive drive = buildDrive();
		String newDir =
				DayOfWeek.from(LocalDate.now()).toString() + "_" +
						System.currentTimeMillis();

		provider.mkdir(drive, newDir);
		assertTrue(provider.exists(drive, newDir), newDir + " not found");
		logger.info("Found newly created directory " + newDir);

		//cleanup
		provider.deleteDirectory(drive, newDir);
		assertFalse(provider.exists(drive, newDir), newDir + " should be deleted, but still exists");
	}

	@Test
	@Tag("integration")
	@Order(82)
	public void testMkdirExistingThrowsException() throws IOException {
		Drive drive = buildDrive();
		String newDir =
				DayOfWeek.from(LocalDate.now()).toString() + "_" +
						System.currentTimeMillis();

		provider.mkdir(drive, newDir);
		assertTrue(provider.exists(drive, newDir), newDir + " not found");
		logger.info("Found newly created directory " + newDir);

		Assertions.assertThrows(FileAlreadyExistsException.class, () ->
			provider.mkdir(drive, newDir)
		);

		//cleanup
		provider.deleteDirectory(drive, newDir);
		assertFalse(provider.exists(drive, newDir), (newDir + " should be deleted, but still exists"));
	}

	@Test
	@Tag("integration")
	@Order(83)
	public void testMkdirSubdir() throws IOException {
		Drive drive = buildDrive();
		String leafDirName = DayOfWeek.from(LocalDate.now()).toString() +
				"_" + System.currentTimeMillis();
		String newDir = PREEXISTING_DIR + "\\" + leafDirName;

		provider.mkdir(drive, newDir);
		assertTrue(provider.exists(drive, newDir), newDir + " not found");
		logger.info("Found newly created directory " + newDir);

		//cleanup
		provider.deleteDirectory(drive, newDir);
		assertFalse(provider.exists(drive, newDir), newDir + " should be deleted, but still exists");
	}

	@Test
	@Tag("integration")
	@Order(101)
	public void testDeleteWindows() throws IOException {
		Drive drive = buildDrive();
		assertTrue(provider.exists(drive, uploadedFileName),
					uploadedFileName + " not found");
		logger.info("Found " + uploadedFileName + " before delete.");
		
		provider.delete(drive, uploadedFileName);

		assertFalse(provider.exists(drive, uploadedFileName),
				uploadedFileName + " still present after attempted deletion");
		logger.info(uploadedFileName + " deleted.");
		
		// Store the deletedFileName so we can attempt
		// a duplicate delete later
		uploadedFileName = null;
	}
	
	@Test
	@Tag("integration")
	public void testGetTopLines() throws Exception {
		File sourceFile = FileUtil.getResourceFile("csvFiles/medications.csv");
		List<String> sourceContents =
			Files.readAllLines(Paths.get(sourceFile.getCanonicalPath()));

		Drive drive = buildDrive();
		int numLines = 10;

		BasicFile topLinesFile =
				provider.getTopLines(drive, sourceFile.getCanonicalPath(), numLines);
		List<String> tlContents = Files.readAllLines(Paths.get(topLinesFile.getCanonicalPath()));
		assertNotNull(tlContents);
		assertEquals(numLines, tlContents.size());

		for (int i = 0; i < numLines; i++) {
			String src = sourceContents.get(i);
			String tl = tlContents.get(i);
			assertEquals(src, tl, StringUtils.difference(src, tl));
		}
	}

	@Test
	@Tag("integration")
	public void testGetTopLinesTooMany() throws Exception {
		File sourceFile = FileUtil.getResourceFile("sizedFiles/5lines.txt");
		List<String> sourceContents =
			Files.readAllLines(Paths.get(sourceFile.getCanonicalPath()));

		Drive drive = buildDrive();
		int numLines = 1000;

		BasicFile topLinesFile =
				provider.getTopLines(drive, sourceFile.getCanonicalPath(), numLines);
		List<String> tlContents = Files.readAllLines(Paths.get(topLinesFile.getCanonicalPath()));
		assertNotNull(tlContents);
		int numTopLines = tlContents.size();
		assertEquals(5, numTopLines);

		for (int i = 0; i < numTopLines; i++) {
			String src = sourceContents.get(i);
			String tl = tlContents.get(i);
			assertEquals(src, tl, "Line " + i + "differed:" +
							StringUtils.difference(src, tl));
		}
	}
	
	@Disabled("Experimental test to see how long it takes" +
	        " to retrieve disk usage using Apache's sizeOfDirectory()")
	@Test
    public void testGetFolderUsage() throws ExplorerException {
        Drive drive = buildCDrive();
        List<DriveProperty> props = drive.getProviderProperties();

        DriveQuery query = new DriveQuery();
        query.setDriveId(drive.getDriveId());
        query.setRecursive(false);
        query.setSearchPattern(""); // get everything

        List<DriveItem> items = provider.find(drive, query);
        int itemCount = items.size();
        logger.info("Found " + itemCount + " drive items using the query: " + query);
        assertTrue(itemCount > 0);

        for (DriveItem item : items) {
            String sPath = "C:/" + item.getPath();
            File file = new File(sPath);

            if (file.isDirectory()) {
                long start = System.currentTimeMillis();
                // It can take a minute or longer to get the size of a dir
                long dSize = FileUtils.sizeOfDirectory(file);
                logger.info("Took " + (System.currentTimeMillis() - start) + " ms " + " to get size of "
                        + file.getName() + " - " + dSize);
            }
        }
    }

	////////// helper methods below

    /**
     * Creates a temporary file with text data to demonstrate
     *  uploading a file.
     * @return A new file with text data.
     * @throws IOException
     */
    File createSampleFile() throws IOException {
            File file = File.createTempFile(FileUtil.MDACA_PREFIX + "testWindows_", ".txt");
            file.deleteOnExit();

            Writer writer = new OutputStreamWriter(new FileOutputStream(file));
            writer.write("Sample file created on " + new Date() + "\n");
            writer.close();
            return file;
    }

	@Override
	protected AbstractStorageProvider getProvider() {
		return new WindowsStorageProvider();
	}
}
