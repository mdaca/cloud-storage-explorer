package com.spinsys.mdaca.storage.explorer.provider;

import static com.spinsys.mdaca.storage.explorer.provider.GoogleCloudStorageProvider.GOOGLE_BUCKET_NAME_PROPERTY_KEY;
import static com.spinsys.mdaca.storage.explorer.provider.GoogleCloudStorageProvider.GOOGLE_CREDENTIALS;
import static com.spinsys.mdaca.storage.explorer.provider.GoogleCloudStorageProvider.PROJECT_PROPERTY_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
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

import com.spinsys.mdaca.storage.explorer.model.exception.ExplorerException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import com.spinsys.mdaca.storage.explorer.io.BasicFile;
import com.spinsys.mdaca.storage.explorer.io.FileUtil;
import com.spinsys.mdaca.storage.explorer.io.PathProcessor;
import com.spinsys.mdaca.storage.explorer.model.DriveItem;
import com.spinsys.mdaca.storage.explorer.model.DriveQuery;
import com.spinsys.mdaca.storage.explorer.model.enumeration.DriveType;
import com.spinsys.mdaca.storage.explorer.model.http.TransferSpec;
import com.spinsys.mdaca.storage.explorer.persistence.Drive;
import com.spinsys.mdaca.storage.explorer.rest.DriveService;

/**
 * Contains integration tests for GoogleCloudStorageProvider
 * 
 * These tests are highly dependent on files and directories being in certain
 * places on the drive, on certain environment variables (see
 * initializeTestDrive()), and on test ordering (@Order).
 */
//TODO - reduce dependencies, further automate initial state restoration

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class GoogleCloudStorageProviderIT extends AbstractProviderIT {

	private static final String EXISTING_DIR = "csv-dir";

	private static final String EMPTY_SUBDIR = EXISTING_DIR + "/EmptySubdir";

	private static final Logger logger = Logger
			.getLogger("com.spinsys.mdaca.storage.explorer.provider.GoogleCloudStorageProviderIT");

	/** A file name for a file that should already exist. */
	private static final String PREEXISTING_FILE = "keithSaysLeaveMeAlone.txt";

	/** A file name for a prefix/directory that should already exist. */
	private static final String PREEXISTING_DIR = EXISTING_DIR;

	DriveService driveService = new DriveService();

	/**
	 * We'll store this file between tests, so we can rename it, delete it, etc. If
	 * we're lucky, we can sequence our tests such that the S3 drive ends up in the
	 * same state it was in the beginning by deleting this uploaded file at the end.
	 */
	private static String uploadedFileName = null;

	/**
	 * We'll store this file between tests, so we can attempt to delete it twice.
	 */
	private static String deletedFileName = null;

	@BeforeAll
	public static void setup() {
	}

	public Drive buildDrive() {
		Drive drive = new Drive();
		drive.setDisplayName("Sample GCS Bucket");
		drive.setDriveType(DriveType.GCS);
		drive.setDriveId(1);
		drive.addPropertyValue(GOOGLE_BUCKET_NAME_PROPERTY_KEY,
				System.getenv(GOOGLE_BUCKET_NAME_PROPERTY_KEY));
		drive.addPropertyValue(PROJECT_PROPERTY_KEY,
				System.getenv(PROJECT_PROPERTY_KEY));
		String gcsJsonFileName = System.getenv(GOOGLE_CREDENTIALS);
        try {
            String creds = FileUtils.readFileToString(new File(gcsJsonFileName), StandardCharsets.UTF_8);
    		drive.addPropertyValue(GOOGLE_CREDENTIALS, creds);
        } catch (IOException e) {
			logger.log(Level.WARNING, e.getMessage(), e);
        }
		return drive;
	}

	@Disabled("Pattern matching not yet implemented")
	@Test
	@Tag("integration")
	public void testFindGarbage() throws IOException {
		Drive drive = buildDrive();

		DriveQuery query = new DriveQuery();
		query.setDriveId(drive.getDriveId());
		query.setRecursive(false);
		query.setSearchPattern(".*yurwufru.*");
		query.setStartPath(EXISTING_DIR); // start at csv-dir

		List<DriveItem> items = provider.find(drive, query);
		int size = items.size();
		logger.info("Found " + size + " results using the query: " + query);
		assertEquals(0, size, "There should be no disk objects matching .*yurwufru.*");
	}

	@Disabled("Pattern matching not yet implemented")
	@Test
	@Tag("integration")
	@Order(2)
	public void testFindRegex() throws IOException {
		Drive drive = buildDrive();

		DriveQuery query = new DriveQuery();
		query.setDriveId(drive.getDriveId());
		query.setRecursive(false);
		query.setSearchPattern(".*Kei.*");
//			query.setSearchPattern(""); // get everything
//			query.setStartPath(null); // start at the root
		query.setStartPath(EXISTING_DIR); // start at csv-dir

		List<DriveItem> items = provider.find(drive, query);
		int size = items.size();
		logger.info("Found " + size + " results using the query: " + query);
		assertTrue(size > 0);
		assertTrue(items.get(0).getPath().contains("Kei"));
	}

	@Test
	@Tag("integration")
	@Order(3)
	public void testFindAllCsvDir() throws IOException {
		Drive drive = buildDrive();

		DriveQuery query = new DriveQuery();
		query.setDriveId(drive.getDriveId());
		query.setRecursive(true);
		query.setSearchPattern(""); // get everything
		query.setStartPath("csv-dir"); // find everything inside csv-dir

		List<DriveItem> itemsAll = provider.find(drive, query);
		int sizeAll = itemsAll.size();
		logger.info("Found " + sizeAll + " results using the query: " + query);
		assertTrue(sizeAll > 0);
	}

	@Test
	@Tag("integration")
	@Order(4)
	public void testFindFromDir() throws IOException {
		Drive drive = buildDrive();

		DriveQuery query = new DriveQuery();
		query.setDriveId(drive.getDriveId());
		query.setRecursive(true);
		query.setSearchPattern(""); // get everything
		query.setStartPath(EXISTING_DIR); // start at csv-dir
		List<DriveItem> itemsCSV = provider.find(drive, query);
		int sizeCSV = itemsCSV.size();
		logger.info("Found " + sizeCSV + " results using the query: " + query);
		assertTrue(sizeCSV > 0);
		assertTrue(itemsCSV.get(0).getPath().contains("csv"));
	}

	@Test
	@Tag("integration")
	@Order(5)
	public void testFlatFindFromEmptyDir() throws IOException {
		Drive drive = buildDrive();

		DriveQuery query = new DriveQuery();
		query.setDriveId(drive.getDriveId());
		query.setRecursive(false);
		query.setSearchPattern(""); // get everything
		query.setStartPath(EMPTY_SUBDIR);
		List<DriveItem> itemsCSV = provider.find(drive, query);
		int sizeCSV = itemsCSV.size();
		logger.info("Found " + sizeCSV + " results using the query: " + query);
		assertEquals(0, sizeCSV, "A flat listing of an empty directory should contain nothing.");
	}

	@Test
	@Tag("integration")
	@Order(6)
	public void testRecursiveFindFromEmptyDir() throws IOException {
		Drive drive = buildDrive();

		DriveQuery query = new DriveQuery();
		query.setDriveId(drive.getDriveId());
		query.setRecursive(true);
		query.setSearchPattern(""); // get everything
		query.setStartPath(EMPTY_SUBDIR);
		List<DriveItem> itemsCSV = provider.find(drive, query);
		int sizeCSV = itemsCSV.size();
		logger.info("Found " + sizeCSV + " results using the query: " + query);
		assertEquals(0, sizeCSV, "A recursive listing of an empty directory should NOT contain the start directory..");
	}

	@Test
	@Tag("integration")
	@Order(10)
	public void testDirectoryExists() throws IOException {
		Drive drive = buildDrive();
		assertTrue(provider.exists(drive, PREEXISTING_DIR),
				   PREEXISTING_DIR + " should be recognized as existing.");
	}

	@Test
	@Tag("integration")
	@Order(20)
	public void testIsDirectoryCsvDir() throws IOException {
		Drive drive = buildDrive();
		assertTrue(provider.isDirectory(drive, PREEXISTING_DIR),
				"A directory should be recognized even" +
				" when there is no slash at the end of the name.");
	}

	@Test
	@Tag("integration")
	@Order(21)
	public void testIsDirectoryCsvDirSlash() throws IOException {
		Drive drive = buildDrive();
		assertTrue(provider.isDirectory(drive, PREEXISTING_DIR + PathProcessor.UNIX_SEP),
				"A directory should be recognized even" + " when there is a slash at the end of the name.");
	}

	@Test
	@Tag("integration")
	@Order(22)
	public void testIsDirectoryCsvDirTrunc() throws IOException {
		Drive drive = buildDrive();
		assertFalse(provider.isDirectory(drive, "csv-"), "Having lots of characters follow some specified"
				+ " characters is not sufficient to consider it a directory.");
	}

	@Test
	@Tag("integration")
	@Order(30)
	public void testUpload() throws IOException {
		Drive drive = buildDrive();

		File sampleFile = createSampleFile();
		String fileName = sampleFile.getName();
		assertFalse(provider.exists(drive, fileName));
		uploadedFileName = sampleFile.getName();
		provider.upload(drive, uploadedFileName, sampleFile);
		assertTrue(provider.exists(drive, fileName));
	}

	@Test
	@Tag("integration")
	@Order(31)
	public void testUploadAlreadyExistingThrowsException() throws IOException {
		Drive drive = buildDrive();

		File sampleFile = createSampleFile();
		String fileName = sampleFile.getName();
		assertFalse(provider.exists(drive, fileName));
		provider.upload(drive, fileName, sampleFile);
		assertTrue(provider.exists(drive, fileName));

		Assertions.assertThrows(FileAlreadyExistsException.class,
								() -> provider.upload(drive, fileName, sampleFile));

		provider.delete(drive, fileName);
		assertFalse(provider.exists(drive, fileName));
	}

	@Disabled("This could take an hour")
	@Test
	@Tag("integration")
	@Order(33)
	public void testUpload5gbFile() throws IOException {
		Drive drive = buildDrive();
		String fileName = "big5gbRandomAccessFile";

		assertFalse(provider.exists(drive, fileName));
		File bigFile = create5GBFile(fileName);
		provider.upload(drive, fileName, bigFile);
		assertTrue(provider.exists(drive, fileName));
	}

	@Test
	@Tag("integration")
	@Order(35)
	public void testDownload() throws IOException {
		Drive drive = buildDrive();
		File downloaded = provider.download(drive, PREEXISTING_FILE);
		assertTrue(downloaded.exists());
		assertTrue(downloaded.canRead());
		assertTrue(downloaded.length() > 10);
	}

	@Test
	@Tag("integration")
	@Order(40)
	public void testRenameUploadedFile() throws IOException {
		Drive drive = buildDrive();

		assertTrue(provider.exists(drive, uploadedFileName),
				   uploadedFileName + " not found");
		logger.info("Found " + uploadedFileName + " before rename.");

		String mvFileName = uploadedFileName + "mv";
		provider.rename(drive, uploadedFileName, mvFileName);
		assertFalse(provider.exists(drive, uploadedFileName),
				    uploadedFileName + " was not renamed");

		assertTrue(provider.exists(drive, mvFileName), mvFileName + " not found");
		logger.info("Found " + mvFileName + " after rename.");

		try {
			provider.rename(drive, PREEXISTING_FILE, mvFileName);
			fail("An attempt to overwrite a file via rename should fail");
		} catch (IOException e) {
			// good - ignore
		}

		// Update the stored file name so we can delete it later
		uploadedFileName = mvFileName;
	}

	@Test
	@Tag("integration")
	@Order(45)
	public void testCopyInDir() throws IOException {
		Drive drive = buildDrive();

		assertTrue(provider.exists(drive, PREEXISTING_FILE), PREEXISTING_FILE + " not found");
		logger.info("Found " + PREEXISTING_FILE + " before copy.");

		TransferSpec spec = new TransferSpec();
		spec.setSourcePath(PREEXISTING_FILE);
		String destPath = PREEXISTING_FILE + "cp";
		spec.setDestPath(destPath);
		((GoogleCloudStorageProvider)provider).copy(drive, spec);

		assertTrue(provider.exists(drive, PREEXISTING_FILE), PREEXISTING_FILE + " should still be present after copy");
		assertTrue(provider.exists(drive, destPath), destPath + " should be present after copy");
		logger.info(PREEXISTING_FILE + " copied to " + destPath);

		// clean up
		provider.delete(drive, destPath);
	}

	@Test
	@Tag("integration")
	@Order(47)
	public void testCopyAcrossDirs() throws IOException {
		Drive drive = buildDrive();

		assertTrue(provider.exists(drive, PREEXISTING_FILE), PREEXISTING_FILE + " not found");
		logger.info("Found " + PREEXISTING_FILE + " before copy.");

		TransferSpec spec = new TransferSpec();
		spec.setSourcePath(PREEXISTING_FILE);
		String destPath = PREEXISTING_DIR + PathProcessor.UNIX_SEP + PREEXISTING_FILE + "cp";
		spec.setDestPath(destPath);
		((GoogleCloudStorageProvider)provider).copy(drive, spec);

		assertTrue(provider.exists(drive, PREEXISTING_FILE), PREEXISTING_FILE + " should still be present after copy");
		assertTrue(provider.exists(drive, destPath), destPath + " should be present after copy");
		logger.info(PREEXISTING_FILE + " copied to " + destPath);

		// clean up
		provider.delete(drive, destPath);
	}

//	@Test
//	@Tag("integration")
//	@Order(48)
//	public void testMoveAcrossDirs() throws IOException {
//		Drive drive = initializeTestDrive();
//
//		try {
//			assertTrue(provider.exists(drive, PREEXISTING_FILE),
//					PREEXISTING_FILE + " not found");
//			logger.info("Found " + PREEXISTING_FILE + " before move.");
//			
//			TransferSpec spec = new TransferSpec();
//			spec.setSourcePath(PREEXISTING_FILE);
//			String destPath = PREEXISTING_DIR + PathProcessor.UNIX_SEP + PREEXISTING_FILE + "mv";
//			spec.setDestPath(destPath);
//			provider.move(drive, spec);
//
//			assertFalse(provider.exists(drive, PREEXISTING_FILE),
//					PREEXISTING_FILE + " should not still be present after move");
//			assertTrue(provider.exists(drive, destPath),
//					destPath + " should be present after move");
//			logger.info(PREEXISTING_FILE + " moved to " + destPath);
//			
//			// clean up
//			spec = new TransferSpec();
//			spec.setSourcePath(destPath);
//			spec.setDestPath(PREEXISTING_FILE);
//			provider.move(drive, spec);
//
//			assertTrue(provider.exists(drive, PREEXISTING_FILE),
//					PREEXISTING_FILE + " should be present after 2nd move");
//			assertFalse(provider.exists(drive, destPath),
//					destPath + " should not still be present after 2nd move");
//			logger.info(destPath + " moved to " + PREEXISTING_FILE);
//		} catch (AmazonClientException e) {
//			printAmazonClientException(e);
//		}
//	}

	@Test
	@Tag("integration")
	@Order(50)
	public void testDelete() throws IOException {
		Drive drive = buildDrive();
		assertTrue(provider.exists(drive, uploadedFileName), uploadedFileName + " not found");
		logger.info("Found " + uploadedFileName + " before delete.");

		provider.delete(drive, uploadedFileName);
		assertFalse(provider.exists(drive, uploadedFileName),
				uploadedFileName + " still present after attempted deletion");
		logger.info(uploadedFileName + " deleted.");

		// Store the deletedFileName so we can attempt
		// a duplicate delete later
		deletedFileName = uploadedFileName;
		uploadedFileName = null;
	}

	@Test
	@Tag("integration")
	@Order(61)
	public void testMkdir() throws IOException {
		Drive drive = buildDrive();
		String newDir = DayOfWeek.from(LocalDate.now()).toString() + "_" + System.currentTimeMillis() + "/";

		provider.mkdir(drive, newDir);
		assertTrue(provider.exists(drive, newDir), newDir + " not found");
		logger.info("Found newly created directory " + newDir);

		// cleanup
		provider.delete(drive, newDir);
	}

	@Test
	@Tag("integration")
	@Order(62)
	public void testMkdirAlreadyExistingThrowsException() throws IOException {
		Drive drive = buildDrive();
		String newDir = DayOfWeek.from(LocalDate.now()).toString() + "_" + System.currentTimeMillis() + "/";

		provider.mkdir(drive, newDir);
		assertTrue(provider.exists(drive, newDir), newDir + " not found");
		logger.info("Found newly created directory " + newDir);

		// A second attempt to create the same directory should fail
		assertThrows(FileAlreadyExistsException.class,
					 () -> provider.mkdir(drive, newDir));

		// cleanup
		provider.delete(drive, newDir);
		assertFalse(provider.exists(drive, newDir), newDir + " is found, but should have deleted");
	}

	@Test
	@Tag("integration")
	@Order(63)
	public void testMkdirSubdir() throws IOException {
		Drive drive = buildDrive();
		String newDir = "csv-dir/" + DayOfWeek.from(LocalDate.now()).toString() + "_" + System.currentTimeMillis()
				+ "/";

		provider.mkdir(drive, newDir);
		assertTrue(provider.exists(drive, newDir), newDir + " not found");
		logger.info("Found newly created directory " + newDir);

		provider.delete(drive, newDir);
		assertFalse(provider.exists(drive, newDir), newDir + " not found");
	}
	

	@Test
	@Tag("integration")
	@Order(71)
	public void testRenameFileFailsBecauseFileAlreadyExists() throws IOException {
		Drive drive = buildDrive();

		File sampleFile1 = createSampleFile();
		String sampleFileName1 = sampleFile1.getName();
		provider.upload(drive, sampleFileName1, sampleFile1);

		File sampleFile2 = createSampleFile();
		String sampleFileName2 = sampleFile2.getName();
		provider.upload(drive, sampleFileName2, sampleFile2);

		Assertions.assertThrows(FileAlreadyExistsException.class,
				() -> provider.rename(drive, sampleFileName1, sampleFileName2));

		provider.delete(drive, sampleFileName1);
		provider.delete(drive, sampleFileName2);

		assertFalse(provider.exists(drive, sampleFileName1));
		assertFalse(provider.exists(drive, sampleFileName2));
	}

	@Test
	@Tag("integration")
	@Order(72)
	public void testRenameDir() throws IOException {
		Drive drive = buildDrive();

		File quotesFolder = new File("src/test/resources/quotesFolder/");
		String quotesFolderName = PathProcessor.addLastSlash(quotesFolder.getName());
		
		try { // make sure directory doesn't already exist
			provider.deleteDirectory(drive, quotesFolderName);
		}
		catch (Exception e) {
			// ignore
		}
		provider.uploadDirectory(drive, quotesFolderName, quotesFolder);

		String unusedDirName = "unusedDirName";
		try { // make sure unusedDirName doesn't already exist
			provider.delete(drive, unusedDirName);
		}
		catch (Exception e) {
			// ignore
		}
		provider.rename(drive, quotesFolderName, unusedDirName);
		assertFalse(provider.exists(drive, quotesFolderName));

		provider.delete(drive, unusedDirName);
		assertFalse(provider.exists(drive, unusedDirName));
	}

	@Test
	@Tag("integration")
	@Order(73)
	public void testRenameDirFailsBecauseDirAlreadyExists() throws IOException {
		Drive drive = buildDrive();

		File quotesFolder = new File("src/test/resources/quotesFolder/");
		String quotesFolderName = PathProcessor.addLastSlash(quotesFolder.getName());

		try { // it's okay if directory already exists
			provider.uploadDirectory(drive, quotesFolderName, quotesFolder);
		}
		catch (Exception e) {
			// ignore
		}

		File sampleFile2 = createSampleFile();
		String sampleFileName2 = sampleFile2.getName();
		provider.upload(drive, sampleFileName2, sampleFile2);

		Assertions.assertThrows(FileAlreadyExistsException.class,
				() -> provider.rename(drive, sampleFileName2, quotesFolderName));

		provider.delete(drive, quotesFolderName);
		provider.delete(drive, sampleFileName2);

		assertFalse(provider.exists(drive, quotesFolderName));
		assertFalse(provider.exists(drive, sampleFileName2));
	}


	@Test
	@Tag("integration")
	@Order(100)
	public void testPartialDownload() throws IOException {
		Drive drive = buildDrive();
		// 10K file - 1K of 0s, followed by 1K of 1s, 1K of 2s, ...
		String path = "numbers.txt";
		provider.downloadPartStart(drive, path);
		byte[] bytes = provider.downloadBytes(drive, path, 5000, 10_000);

		String s = new String(bytes, StandardCharsets.UTF_8);
		logger.info("downloaded '" + s + "'");
		assertEquals(5000, s.length());
		assertEquals(s.charAt(0), '5');
		assertEquals(s.charAt(999), '5');
		assertEquals(s.charAt(1000), '6');
		provider.downloadComplete(drive, path);
	}
	

	@Test
	@Tag("integration")
	@Order(110)
	public void testByteUpload() throws IOException {
		Drive drive = buildDrive();

		// 10K file - 1K of 0s, followed by 1K of 1s, 1K of 2s, ...
		String readFilePath = "csv-dir/numbers.txt";

		if (!provider.exists(drive, readFilePath)) {
			File numbersFile = FileUtil.getResourceFile("numbers.txt");
			provider.upload(drive, readFilePath, numbersFile);
		}

		String writeFilePath = "newFile.txt";
		if (provider.exists(drive, writeFilePath)) {
			provider.delete(drive, writeFilePath);
		}

		provider.downloadPartStart(drive, readFilePath);
		provider.uploadPartStart(drive, writeFilePath);

		byte[] bytes = provider.downloadBytes(drive, readFilePath, 0, 5000);
		provider.uploadPart(drive, writeFilePath, bytes, 0);

		String s = new String(bytes);
		assertEquals(s.charAt(500), '0');
		assertEquals(s.charAt(1500), '1');
		assertEquals(s.charAt(2500), '2');
		assertEquals(s.charAt(3500), '3');
		assertEquals(s.charAt(4500), '4');

		bytes = provider.downloadBytes(drive, readFilePath, 5001, 10000);
		s = new String(bytes);

		provider.uploadPart(drive, writeFilePath, bytes, 0);
		assertEquals(s.charAt(500), '5');
		assertEquals(s.charAt(1500), '6');
		assertEquals(s.charAt(2500), '7');
		assertEquals(s.charAt(3500), '8');
		assertEquals(s.charAt(4500), '9');

		provider.uploadPartComplete(drive, readFilePath, "");

		provider.delete(drive, readFilePath);
		provider.delete(drive, writeFilePath);
	}
	
	@Disabled("Inherited test is misbehaving - not sure why")
    @Test
    @Order(120)
    public void voidTestDeleteWherePlaceholderFileIsMissing() throws IOException {
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
				provider.getTopLines(drive, "csv-dir/medications.csv", numLines);
		List<String> tlContents = Files.readAllLines(Paths.get(topLinesFile.getCanonicalPath()));
		assertNotNull(tlContents);
		assertEquals(numLines, tlContents.size());

		for (int i = 0; i < numLines; i++) {
			String src = sourceContents.get(i);
			String tl = tlContents.get(i);
			assertEquals(src, tl, "Line " + i + " differed: " +
					StringUtils.difference(src, tl));
		}
	}

	////////// helper methods below

	/**
	 * Creates a temporary file with text data to demonstrate uploading a file to
	 * Amazon S3
	 * 
	 * @return A new file with text data.
	 * @throws IOException
	 */
	File createSampleFile() throws IOException {
		File file = File.createTempFile(FileUtil.MDACA_PREFIX + "testGCS_", ".txt");
		file.deleteOnExit();

		Writer writer = new OutputStreamWriter(new FileOutputStream(file));
		writer.write("Sample file created on " + new Date() + "\n");
		writer.close();
		return file;
	}

	private File create5GBFile(String fileName) throws IOException {
		File file = new File(fileName);
		file.createNewFile();

		RandomAccessFile raf = new RandomAccessFile(file, "rw");
		// AWS S3 has a 5GB limit for some operations, so we make it slightly larger
		raf.setLength(5L * FileUtils.ONE_GB + FileUtils.ONE_KB);
		raf.close();
		return file;
	}

	public AbstractStorageProvider getProvider() {
		if (provider == null) {
			provider = new GoogleCloudStorageProvider();
		}
		return provider;
	}

}
