package com.spinsys.mdaca.storage.explorer.provider;

import static com.spinsys.mdaca.storage.explorer.provider.BasicStorageProvider.DRIVE_NAME_PROPERTY_KEY;
import static com.spinsys.mdaca.storage.explorer.provider.SMBStorageProvider.DOMAIN_PROPERTY_KEY;
import static com.spinsys.mdaca.storage.explorer.provider.SMBStorageProvider.HOST_NAME_PROPERTY_KEY;
import static com.spinsys.mdaca.storage.explorer.provider.SMBStorageProvider.PASSWORD_PROPERTY_KEY;
import static com.spinsys.mdaca.storage.explorer.provider.SMBStorageProvider.SHARE_NAME_PROPERTY_KEY;
import static com.spinsys.mdaca.storage.explorer.provider.SMBStorageProvider.USER_NAME_PROPERTY_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.spinsys.mdaca.storage.explorer.model.enumeration.DriveType;
import com.spinsys.mdaca.storage.explorer.persistence.Drive;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import com.spinsys.mdaca.storage.explorer.io.FileUtil;
import com.spinsys.mdaca.storage.explorer.io.PathProcessor;
import com.spinsys.mdaca.storage.explorer.model.DriveItem;
import com.spinsys.mdaca.storage.explorer.model.DriveQuery;
import com.spinsys.mdaca.storage.explorer.rest.DriveService;

/**
 * Contains integration tests for BasicStorageProvider
 * 
 * These tests are highly dependent on files and directories
 * being in certain places on the drive, on certain environment
 * variables (see initializeTestDrive()), and on test ordering
 * (@Order).
 */
// TODO - reduce dependencies, further automate initial state restoration

@TestMethodOrder(OrderAnnotation.class)
public class BasicStorageProviderIT {
	
	/** A file name for a local empty file. */
	private static final String EMPTY_FILE = "sizedFiles/emptyFile.txt";

	/** A name for a directory that should already exist. */
	private static final String PREEXISTING_DIR = "/Temp/csv-dir";

	/** A file name for a file that should already exist. */
	private static final String PREEXISTING_FILE = "/Temp/leaveMeAlone.txt";

	/** A file name for a file that should already exist in a subdirectory. */
	private static final String PREEXISTING_SUBDIR_FILE =
			PREEXISTING_DIR + "\\readme.txt";

	/** A file name for a file that should already exist in a subdirectory,
	 * using a "/" for the file separator. */
	private static final String PREEXISTING_SUBDIR_FILE_SLASH =
			PREEXISTING_DIR + "/readme.txt";

    private static final Logger logger =
            Logger.getLogger("com.spinsys.mdaca.storage.explorer.provider.BasicStorageProviderIT");

	/** We'll store this file between tests, so we can rename it,
	 * delete it, etc.  If we're lucky, we can sequence our tests
	 * such that the Basic drive ends up in the same state it was
	 * in the beginning by deleting this uploaded file at the end.
	 */
	private static String uploadedFileName = null;
	
	static {
		File dirFile = new File(PREEXISTING_FILE);
		dirFile.mkdirs();
		File preFile = new File(PREEXISTING_FILE);
		
		if (!preFile.exists()) {
			try {
				preFile.getParentFile().mkdirs();
				FileUtils.writeStringToFile(preFile, "Integration tests are going to depend on this file's existence, so don't rename or delete it.", StandardCharsets.UTF_8);
			} catch (IOException e) {
				logger.log(Level.WARNING, e.getMessage());
			}
		}
	}

	BasicStorageProvider provider = new BasicStorageProvider();
	

	public static Drive initializeTestDriveWindows() {
		Drive drive = new Drive();
		drive.setDisplayName("Basic_Share");
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
		String driveName = "C"; // System.getenv(DRIVE_NAME_PROPERTY_KEY);
		drive.addPropertyValue(DRIVE_NAME_PROPERTY_KEY,
				(driveName == null || driveName.isEmpty() ? "C" : driveName));
		return drive;
	}

	@Test
	@Tag("integration")
	@Order(100)
	public void testExistsFile() throws IOException {
		Drive drive = initializeTestDriveWindows();

		assertTrue(provider.exists(drive, PREEXISTING_FILE),
				PREEXISTING_FILE + " should exist on " + drive);
	}

	@Test
	@Tag("integration")
	@Order(110)
	public void testExistsDir() throws IOException {
		Drive drive = initializeTestDriveWindows();

		assertTrue(provider.exists(drive, PREEXISTING_DIR),
				PREEXISTING_DIR + " should exist on " + drive);
	}

	@Test
	@Tag("integration")
	@Order(120)
	public void testExistsSubdirFile() throws IOException {
		Drive drive = initializeTestDriveWindows();

		assertTrue(provider.exists(drive, PREEXISTING_SUBDIR_FILE),
				PREEXISTING_SUBDIR_FILE + " should exist on " + drive);
	}

	@Test
	@Tag("integration")
	@Order(130)
	public void testExistsFalse() throws IOException {
		Drive drive = initializeTestDriveWindows();

		String path = "ybgfuyc76";
		assertFalse(provider.exists(drive, path),
				path + " should not exist on " + drive);
	}

	@Test
	@Tag("integration")
	@Order(140)
	public void testEnsureSubdirsExist() throws IOException {
		Drive drive = initializeTestDriveWindows();
		String newDirName =
				PathProcessor.addLastSlash(PREEXISTING_DIR) +
				"newSubdir" + System.currentTimeMillis();
		String newFileName = newDirName + "/newFile.txt";

		assertTrue(provider.exists(drive, PREEXISTING_DIR),
				PREEXISTING_DIR + " should exist on " + drive);
		assertFalse(provider.exists(drive, newDirName),
				newDirName + " should not exist on " + drive);
		provider.ensureDirsExist(drive, newFileName);
		assertTrue(provider.exists(drive, newDirName),
				newDirName + " should exist on " + drive);
		// clean up
		provider.deleteDirectory(drive, newDirName);
	}

	@Test
	@Tag("integration")
	public void testDownloadFileNormal() throws IOException {
		Drive drive = initializeTestDriveWindows();

		File download = provider.download(drive, PREEXISTING_FILE);
		assertTrue(provider.exists(drive, download.getCanonicalPath()),
				PREEXISTING_FILE + " should have downloaded");
		// clean up
		download.delete();
	}

	@Test
	@Tag("integration")
	public void testDownloadFileMissing() throws IOException {
		Drive drive = initializeTestDriveWindows();

		DriveQuery query = new DriveQuery();
		query.setDriveId(drive.getDriveId());
		String path = "ifyufqvf";
		File download = provider.download(drive, path);
		assertNull(download, "Attempt to download non-existent " + path +
				" should have returned null.");
	}
	
	@Test
	@Tag("integration")
	public void testDownloadBytes() throws IOException {
		Drive drive = initializeTestDriveWindows();

		int numBytes = 5;
		byte[] downloaded = provider.downloadBytes(drive, PREEXISTING_FILE, 0, numBytes);
		assertEquals(numBytes, downloaded.length);
		assertEquals("Integ", new String(downloaded));

		numBytes = 4;
		byte[] downloaded2 = provider.downloadBytes(drive, PREEXISTING_FILE, 3, numBytes);
		assertEquals(numBytes, downloaded2.length);
		assertEquals("egra", new String(downloaded2));
	}

	@Test
	@Tag("integration")
	public void testDownloadBytesFromEmptyFile() throws IOException {
		Drive drive = initializeTestDriveWindows();
		int numBytes = 5;
		File inputFile = FileUtil.getResourceFile(EMPTY_FILE);
		String path = inputFile.getCanonicalPath();
		byte[] downloaded = provider.downloadBytes(drive, path, 0, numBytes);
		assertEquals(0, downloaded.length);
	}

	/**
	 * Testing an attempt to find a file that doesn't exist.
	 * @throws IOException
	 */
	@Test
	@Tag("integration")
	public void testFindGarbageRecursive() throws IOException {
		Drive drive = initializeTestDriveWindows();
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


	@Disabled("Takes too long to find everything from root")
	@Test
	@Tag("integration")
	@Order(220)
	public void testFindFromRootUsingRegex() throws IOException {
		Drive drive = initializeTestDriveWindows();

		DriveQuery query = new DriveQuery();
		query.setDriveId(drive.getDriveId());
		query.setRecursive(true);
		query.setSearchPattern(".*txt.*");
//			query.setSearchPattern(""); // get everything
		query.setStartPath(null); // start at the root
//			query.setStartPath(PREEXISTING_DIR); // start at csv-dir
		
		List<DriveItem> items = provider.find(drive, query);
		boolean txtMatch = items.stream().anyMatch(di -> di.getPath().contains("txt"));
		assertTrue(txtMatch);
	}

	@Test
	@Tag("integration")
	public void testFindFromDirNoPatternOneLevel() throws IOException {
		Drive drive = initializeTestDriveWindows();
		File inputFile = FileUtil.getResourceFile("sampleFolder");

		DriveQuery query = new DriveQuery();
		query.setDriveId(drive.getDriveId());
		query.setRecursive(false);
		query.setSearchPattern("");
		String startPath = PathProcessor.removeDriveLetter(inputFile.getCanonicalPath());
		startPath = PathProcessor.convertToUnixStylePath(startPath);
		query.setStartPath(startPath);
		
		List<DriveItem> items = provider.find(drive, query);
		assertEquals(4, items.size());
	}

	@Disabled("Takes too long to find everything from root")
	@Test
	@Tag("integration")
	@Order(230)
	public void testFindAll() throws IOException {
		Drive drive = initializeTestDriveWindows();

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
	@Order(240)
	public void testFromDir() throws IOException {
		Drive drive = initializeTestDriveWindows();

		DriveQuery query = new DriveQuery();
		query.setDriveId(drive.getDriveId());
		query.setRecursive(true);

		query.setStartPath(PREEXISTING_DIR); // start at csv-dir		
		List<DriveItem> itemsCSV = provider.find(drive, query);
		int sizeCSV = itemsCSV.size();
		logger.info("Found " + sizeCSV + " results using the query: " + query);
		assertTrue(sizeCSV > 0);
		assertTrue(itemsCSV.get(0).getPath().contains("csv"));
	}

	@Test
	@Tag("integration")
	@Order(500)
	public void testUpload() throws IOException {
		Drive drive = initializeTestDriveWindows();
		
		File sampleFile = createSampleFile();
		String fileName = sampleFile.getCanonicalPath() + "uploaded";
		provider.upload(drive, fileName, sampleFile);

		assertTrue(provider.exists(drive, fileName));

		provider.delete(drive, fileName);
		assertFalse(provider.exists(drive, fileName));
	}

	@Test
	@Tag("integration")
	@Order(510)
	public void testUploadExistingThrowsException() throws IOException {
		Drive drive = initializeTestDriveWindows();

		File sampleFile = createSampleFile();
		String fileName = sampleFile.getCanonicalPath() + "uploaded";
		provider.upload(drive, fileName, sampleFile);

		assertTrue(provider.exists(drive, fileName));

		assertThrows(FileAlreadyExistsException.class, () ->
				provider.upload(drive, fileName, sampleFile)
		);

		provider.delete(drive, fileName);
		assertFalse(provider.exists(drive, fileName));
	}

	@Tag("integration")
	@Order(520)
	@Test
	public void testUploadDirectory() throws IOException {
		String dirName = "src/test/resources/quotesFolder";
		File dir = new File(dirName);

		Drive drive = initializeTestDriveWindows();
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
	@Order(540)
	public void testUploadPart1() throws IOException {
		Drive drive = initializeTestDriveWindows();

		File inputFile = createSampleFile();
		String inputFileName = inputFile.getName();
		byte[] bytes = Files.readAllBytes(Paths.get(inputFile.getCanonicalPath()));

		String sDestFile =
				PathProcessor.addLastSlash(PREEXISTING_DIR) + inputFileName;
		assertFalse(provider.exists(drive, sDestFile));

		String uploadId = provider.uploadPartStart(drive, sDestFile);
		provider.uploadPart(drive, sDestFile, bytes, 1);
		provider.uploadPartComplete(drive, sDestFile, uploadId);
		assertTrue(provider.exists(drive, sDestFile));

		String inputContents =
				FileUtils.readFileToString(inputFile, StandardCharsets.UTF_8);
		String outputContents =
				FileUtils.readFileToString(new File(sDestFile),
											StandardCharsets.UTF_8);
		assertEquals(inputContents, outputContents);

		// clean up
		provider.delete(drive, sDestFile);
		assertFalse(provider.exists(drive, sDestFile));
	}

	@Test
	@Tag("integration")
	@Order(541)
	public void testUploadPart2() throws IOException {
		Drive drive = initializeTestDriveWindows();
		File inputFile = createSampleFile();
		String inputFileName = inputFile.getName();
		String sDestFile =
				PathProcessor.addLastSlash(PREEXISTING_DIR) + inputFileName;
		assertFalse(provider.exists(drive, sDestFile));

		String uploadId = provider.uploadPartStart(drive, sDestFile);

		// Upload 1st chunk
		int size = (int) (5 * FileUtils.ONE_MB);
		String s1 = StringUtils.rightPad("testUploadPart2\n", size, "*");
		provider.uploadPart(drive, sDestFile, s1.getBytes(), 1);

		// Upload 2nd chunk
		byte[] bytes = Files.readAllBytes(Paths.get(inputFile.getCanonicalPath()));
		provider.uploadPart(drive, sDestFile, bytes, 2);

		provider.uploadPartComplete(drive, sDestFile, uploadId);
		assertTrue(provider.exists(drive, sDestFile));

		String inputContents = s1 +
				FileUtils.readFileToString(inputFile, StandardCharsets.UTF_8);
		String outputContents =
				FileUtils.readFileToString(new File(sDestFile),
											StandardCharsets.UTF_8);
		assertEquals(inputContents, outputContents);

		// clean up
		provider.delete(drive, sDestFile);
		assertFalse(provider.exists(drive, sDestFile));
	}

	@Test
	@Tag("integration")
	@Order(542)
	public void testUploadPartEmptyFile() throws IOException {
		Drive drive = initializeTestDriveWindows();

		File inputFile = FileUtil.getResourceFile(EMPTY_FILE);
		String inputFileName = inputFile.getName();
		String sDestFile =
				PathProcessor.addLastSlash(PREEXISTING_DIR) + inputFileName;
		assertFalse(provider.exists(drive, sDestFile));

		String uploadId = provider.uploadPartStart(drive, sDestFile);
		byte[] bytes = Files.readAllBytes(Paths.get(inputFile.getCanonicalPath()));
		provider.uploadPart(drive, sDestFile, bytes, 1);
		provider.uploadPartComplete(drive, sDestFile, uploadId);
		assertTrue(provider.exists(drive, sDestFile));

		// clean up
		provider.delete(drive, sDestFile);
		assertFalse(provider.exists(drive, sDestFile));
	}


	@Test
	@Tag("integration")
	@Order(600)
	public void testRename() throws IOException {
		Drive drive = initializeTestDriveWindows();

		File sampleFile = createSampleFile();
		String fileName = sampleFile.getCanonicalPath() + "uploaded";
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
	@Order(610)
	public void testRenameSubdirFile() throws IOException {
		Drive drive = initializeTestDriveWindows();

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
	@Order(610)
	public void testRenameSubdirFileSlash() throws IOException {
		Drive drive = initializeTestDriveWindows();

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
	@Order(810)
	public void testMkdir() throws IOException {
		Drive drive = initializeTestDriveWindows();
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
	@Order(820)
	public void testMkdirExistingThrowsException() throws IOException {
		Drive drive = initializeTestDriveWindows();
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
	@Order(830)
	public void testMkdirSubdir() throws IOException {
		Drive drive = initializeTestDriveWindows();
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
	@Order(1000)
	public void testDelete() throws IOException {
		Drive drive = initializeTestDriveWindows();
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
	public void testGetInputStream() throws IOException {
		Drive drive = initializeTestDriveWindows();

		int numBytes = 5;
		File inputFile = FileUtil.getResourceFile("csvFiles/medications.csv");
		try (InputStream inputStream = provider.getInputStream(drive, inputFile.getCanonicalPath())) {
			byte[] bytes = new byte[numBytes];
			inputStream.read(bytes);
			assertEquals(numBytes, bytes.length);
			assertEquals("START", new String(bytes));
			
			inputStream.read(bytes);
			assertEquals(numBytes, bytes.length);
			assertEquals(",STOP", new String(bytes));
			
			inputStream.read(bytes, 1, 2);
			assertEquals(numBytes, bytes.length);
			assertEquals(",,POP", new String(bytes));
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
            File file = File.createTempFile(FileUtil.MDACA_PREFIX + "testBasic_", ".txt");
            file.deleteOnExit();

            Writer writer = new OutputStreamWriter(new FileOutputStream(file));
            writer.write("Sample file created on " + new Date() + "\n");
            writer.close();
            return file;
    }
}
