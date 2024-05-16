package com.spinsys.mdaca.storage.explorer.provider;

import static com.spinsys.mdaca.storage.explorer.provider.AWSS3StorageProvider.ACCESS_KEY_PROPERTY_KEY;
import static com.spinsys.mdaca.storage.explorer.provider.AWSS3StorageProvider.ACCESS_SECRET_PROPERTY_KEY;
import static com.spinsys.mdaca.storage.explorer.provider.AWSS3StorageProvider.BUCKET_NAME_PROPERTY_KEY;
import static com.spinsys.mdaca.storage.explorer.provider.AWSS3StorageProvider.GLACIER;
import static com.spinsys.mdaca.storage.explorer.provider.AWSS3StorageProvider.REGION_PROPERTY_KEY;
import static com.spinsys.mdaca.storage.explorer.provider.AWSS3StorageProvider.STANDARD_IA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.io.Writer;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.util.Date;
import java.util.List;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

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
import org.mockito.Mockito;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.spinsys.mdaca.storage.explorer.io.BasicFile;
import com.spinsys.mdaca.storage.explorer.io.FileUtil;
import com.spinsys.mdaca.storage.explorer.io.PathProcessor;
import com.spinsys.mdaca.storage.explorer.model.DriveItem;
import com.spinsys.mdaca.storage.explorer.model.DriveQuery;
import com.spinsys.mdaca.storage.explorer.model.StorageClass;
import com.spinsys.mdaca.storage.explorer.model.enumeration.DriveType;
import com.spinsys.mdaca.storage.explorer.model.exception.ExplorerException;
import com.spinsys.mdaca.storage.explorer.model.http.FileLocationSpec;
import com.spinsys.mdaca.storage.explorer.model.http.TransferSpec;
import com.spinsys.mdaca.storage.explorer.persistence.Drive;
import com.spinsys.mdaca.storage.explorer.rest.DriveService;

/**
 * Contains integration tests for AWSS3StorageProvider
 *
 * These tests are highly dependent on files and directories
 * being in certain places on the drive, on certain environment
 * variables (see initializeTestDrive()), and on test ordering
 * (@Order).
 */
//TODO - reduce dependencies, further automate initial state restoration

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class AWSS3StorageProviderIT extends AbstractProviderIT {

    private static final Logger logger =
            Logger.getLogger("com.spinsys.mdaca.storage.explorer.provider.AWSS3StorageProviderIT");

	/** A file name for a local empty file. */
	private static final String EMPTY_FILE = "sizedFiles/emptyFile.txt";

	/** A file name for a pre-existing empty file on AWS S3. */
	private static final String EMPTY_FILE_S3 = "keithSaysLeaveMeEmpty.txt";

	/** A file name for a file that should already exist on AWS S3. */
	private static final String PREEXISTING_FILE = "keithSaysLeaveMeAlone.txt";

	/** A file name for a prefix/directory that should already exist. */
	private static final String PREEXISTING_DIR = "csv-dir/";

	AWSS3StorageProvider provider = new AWSS3StorageProvider();

	DriveService driveService = new DriveService();

	private static HttpServletRequest request;

	private static HttpServletResponse response;

	/** We'll store this file between tests, so we can rename it,
	 * delete it, etc.  If we're lucky, we can sequence our tests
	 * such that the S3 drive ends up in the same state it was
	 * in the beginning by deleting this uploaded file at the end.
	 */
	private static String uploadedFileName = null;


	@BeforeAll
	public static void setup() {
		request = Mockito.mock(HttpServletRequest.class);
		response = Mockito.mock(HttpServletResponse.class);
	}

	public Drive buildDrive() {
		Drive drive = new Drive();
		drive.setDisplayName("Sample S3 Bucket");
		drive.setDriveType(DriveType.S3);
		drive.setDriveId(1);
		drive.addPropertyValue(ACCESS_KEY_PROPERTY_KEY,
				System.getenv(ACCESS_KEY_PROPERTY_KEY));
		drive.addPropertyValue(ACCESS_SECRET_PROPERTY_KEY,
				System.getenv(ACCESS_SECRET_PROPERTY_KEY));
		drive.addPropertyValue(BUCKET_NAME_PROPERTY_KEY,
				System.getenv(BUCKET_NAME_PROPERTY_KEY));
		drive.addPropertyValue(REGION_PROPERTY_KEY,
	               "us-east-1");
		return drive;
	}

	@Test
	@Tag("integration")
	public void testFindGarbage() throws IOException {
		Drive drive = buildDrive();

		try {
			DriveQuery query = new DriveQuery();
			query.setDriveId(drive.getDriveId());
			query.setRecursive(false);
			query.setSearchPattern(".*yurwufru.*");
			query.setStartPath(PREEXISTING_DIR); // start at csv-dir

			List<DriveItem> items = provider.find(drive, query);
			int size = items.size();
			logger.info("Found " + size + " results using the query: " + query);
			assertEquals(0, size, "There should be no disk objects matching .*yurwufru.*");
		} catch (AmazonServiceException ase) {
			printAmazonServiceException(ase);
		} catch (AmazonClientException ace) {
			printAmazonClientException(ace);
		}
	}

	@Test
	@Tag("integration")
	@Order(20)
	public void testFindRegex() throws IOException {
		Drive drive = buildDrive();

		try {
			DriveQuery query = new DriveQuery();
			query.setDriveId(drive.getDriveId());
			query.setRecursive(false);
			query.setSearchPattern(".*Kei.*");
//			query.setSearchPattern(""); // get everything
//			query.setStartPath(null); // start at the root
			query.setStartPath(PREEXISTING_DIR); // start at csv-dir

			List<DriveItem> items = provider.find(drive, query);
			int size = items.size();
			logger.info("Found " + size + " results using the query: " + query);
			assertTrue(size > 0);
			
			for (DriveItem item : items) {
				assertTrue(item.getPath().contains("Kei"),
						item.getFileName() + " should have contained 'Kei'.");
			}
		} catch (AmazonServiceException ase) {
			printAmazonServiceException(ase);
		} catch (AmazonClientException ace) {
			printAmazonClientException(ace);
		}
	}

	@Test
	@Tag("integration")
	@Order(30)
	public void testFindTxtFileInCsvDir() throws IOException {
		Drive drive = buildDrive();

		try {
			DriveQuery query = new DriveQuery();
			query.setDriveId(drive.getDriveId());
			query.setRecursive(true);
			query.setSearchPattern(""); // get everything

			query.setSearchPattern(".*txt.*");
			query.setStartPath(PREEXISTING_DIR); // start at csv-dir

			List<DriveItem> itemsSome = provider.find(drive, query);
			int sizeSome = itemsSome.size();
			logger.info("Found " + sizeSome + " results using the query: " + query);
			assertTrue(sizeSome > 0);
		} catch (AmazonServiceException ase) {
			printAmazonServiceException(ase);
		} catch (AmazonClientException ace) {
			printAmazonClientException(ace);
		}

	}

	@Test
	@Tag("integration")
	@Order(40)
	public void testFindAllInCsvDir() throws IOException {
		Drive drive = buildDrive();

		try {
			DriveQuery query = new DriveQuery();
			query.setDriveId(drive.getDriveId());
			query.setRecursive(true);
			query.setSearchPattern(""); // get everything

			query.setStartPath(PREEXISTING_DIR); // start at csv-dir
			List<DriveItem> itemsCSV = provider.find(drive, query);
			int sizeCSV = itemsCSV.size();
			logger.info("Found " + sizeCSV + " results using the query: " + query);
			assertTrue(sizeCSV > 0);
			assertTrue(itemsCSV.get(0).getPath().contains("csv"));
		} catch (AmazonServiceException ase) {
			printAmazonServiceException(ase);
		} catch (AmazonClientException ace) {
			printAmazonClientException(ace);
		}

	}

	@Test
	@Order(100)
	public void testTestConnectionSuccess() throws ExplorerException {
		Drive drive = buildDrive();

		assertTrue(provider.testConnection(drive), "tested drive connection with valid properties, " +
				"but returned invalid connection");
	}

	@Test
	@Order(110)
	public void testTestConnectionInvalid() throws ExplorerException {
		Drive drive = buildDrive();

		try {
	        drive.setProviderProperties(null);
	        provider.testConnection(drive);
	        fail("tested drive connection with no properties " +
	                drive +", but still returned successful connection");
		}
		catch (Exception e) {
		    // good
		}
	}	

	@Test
	@Tag("integration")
	@Order(200)
	public void testIsDirectoryCsvDir() {
		Drive drive = buildDrive();
		assertTrue(provider.isDirectory(drive, PREEXISTING_DIR),
				"A directory should be recognized even"
				+ " when there is no slash at the end of the name.");
	}

	@Test
	@Tag("integration")
	@Order(210)
	public void testIsDirectoryCsvDirSlash() {
		Drive drive = buildDrive();
		assertTrue(provider.isDirectory(drive,
								PathProcessor.addLastSlash(PREEXISTING_DIR)),
				"A directory should be recognized even"
				+ " when there is a slash at the end of the name.");
	}

	@Test
	@Tag("integration")
	@Order(220)
	public void testIsDirectoryCsvDirTrunc() {
		Drive drive = buildDrive();
		assertFalse(provider.isDirectory(drive, "csv-"),
				"Having lots of characters follow some specified"
				+ " characters is not sufficient to consider it a directory.");
	}

	@Test
	@Tag("integration")
	@Order(300)
	public void testUpload() throws IOException {
		Drive drive = buildDrive();

		try {
			File sampleFile = createSampleFile();
			String fileName = sampleFile.getName();
			assertFalse(provider.exists(drive, fileName));
			uploadedFileName = sampleFile.getName();
			provider.upload(drive, uploadedFileName, sampleFile);
			assertTrue(provider.exists(drive, fileName));

		} catch (AmazonServiceException ase) {
			printAmazonServiceException(ase);
		} catch (AmazonClientException ace) {
			printAmazonClientException(ace);
		}
	}

	@Test
	@Tag("integration")
	@Order(310)
	public void testUploadAlreadyExistingThrowsException() throws IOException {
		Drive drive = buildDrive();

		try {
			File sampleFile = createSampleFile();
			String fileName = sampleFile.getName();
			assertFalse(provider.exists(drive, fileName));
			provider.upload(drive, fileName, sampleFile);
			assertTrue(provider.exists(drive, fileName));

			Assertions.assertThrows(FileAlreadyExistsException.class, () ->
					provider.upload(drive, fileName, sampleFile)
			);

			// clean up
			provider.delete(drive, fileName);
			assertFalse(provider.exists(drive, fileName));
		} catch (AmazonServiceException ase) {
			printAmazonServiceException(ase);
		} catch (AmazonClientException ace) {
			printAmazonClientException(ace);
		}
	}

	@Disabled("This could take an hour")
	@Test
	@Tag("integration")
	@Order(330)
	public void testUpload5gbFile() throws IOException {
		Drive drive = buildDrive();
		String fileName = "big5gbRandomAccessFile";
		File bigFile = null;

		try {
			assertFalse(provider.exists(drive, fileName));
			bigFile = create5GBFile(fileName);
			provider.upload(drive, fileName, bigFile);
			assertTrue(provider.exists(drive, fileName));
		} catch (AmazonServiceException ase) {
			printAmazonServiceException(ase);
		} catch (AmazonClientException ace) {
			printAmazonClientException(ace);
		}
		finally {
			// clean up
			if (bigFile != null) {
				bigFile.delete();
			}
		}
	}
	
	@Test
	@Tag("integration")
	@Order(300)
	public void testUploadPart1() throws IOException {
		Drive drive = buildDrive();

		try {
			File sampleFile = createSampleFile();
			String fileName = sampleFile.getName();
			assertFalse(provider.exists(drive, fileName));
			
			String uploadId = provider.uploadPartStart(drive, fileName);
			byte[] bytes = Files.readAllBytes(Paths.get(sampleFile.getCanonicalPath()));
			provider.uploadPart(drive, fileName, bytes, 1);
			provider.uploadPartComplete(drive, fileName, uploadId);
			assertTrue(provider.exists(drive, fileName));
		} catch (AmazonServiceException ase) {
			printAmazonServiceException(ase);
		} catch (AmazonClientException ace) {
			printAmazonClientException(ace);
		}
	}


	@Test
	@Tag("integration")
	@Order(301)
	public void testUploadPart2() throws IOException {
		Drive drive = buildDrive();

		try {
			File sampleFile = createSampleFile();
			String fileName = sampleFile.getName();
			assertFalse(provider.exists(drive, fileName));
			
			String uploadId = provider.uploadPartStart(drive, fileName);

			// All chunks must be >= 5 MB, except for the last one
			int size = (int) (5 * FileUtils.ONE_MB);
			String s1 = StringUtils.rightPad("testUploadPart2\n", size, "*");
			provider.uploadPart(drive, fileName, s1.getBytes(), 1);
			
			byte[] bytes = Files.readAllBytes(Paths.get(sampleFile.getCanonicalPath()));
			provider.uploadPart(drive, fileName, bytes, 2);
			provider.uploadPartComplete(drive, fileName, uploadId);
			assertTrue(provider.exists(drive, fileName));

			// clean up
			provider.delete(drive, fileName);
			assertFalse(provider.exists(drive, fileName));
		} catch (AmazonServiceException ase) {
			printAmazonServiceException(ase);
		} catch (AmazonClientException ace) {
			printAmazonClientException(ace);
		}
	}

	@Test
	@Tag("integration")
	@Order(302)
	public void testUploadPartEmptyFile() throws IOException {
		Drive drive = buildDrive();

		try {
			File emptyFile = FileUtil.getResourceFile(EMPTY_FILE);
			String fileName = emptyFile.getName();
			assertFalse(provider.exists(drive, fileName));
			
			String uploadId = provider.uploadPartStart(drive, fileName);
			byte[] bytes = Files.readAllBytes(Paths.get(emptyFile.getCanonicalPath()));
			provider.uploadPart(drive, fileName, bytes, 1);
			provider.uploadPartComplete(drive, fileName, uploadId);
			assertTrue(provider.exists(drive, fileName));

			// clean up
			provider.delete(drive, fileName);
			assertFalse(provider.exists(drive, fileName));
		} catch (AmazonServiceException ase) {
			printAmazonServiceException(ase);
		} catch (AmazonClientException ace) {
			printAmazonClientException(ace);
		}
	}

	@Disabled("This could take an hour or more")
	@Test
	@Tag("integration")
	@Order(330)
	public void testCompareUploadTimes() throws IOException {
		Drive drive = buildDrive();
		int numChunks = 100;
		int chunkSize = (int) (10 * FileUtils.ONE_MB);
		int fileSize = numChunks * chunkSize;

		// default multiPartUploadThreshold =  16_777_216
		// default multiPartUploadPartSize =    5_242_880
		// default multiPartCopyThreshold = 5_368_709_120
		// default multiPartCopyPartSize =    104_857_600
		String baseFileName = "uploadTest_" + fileSize + "_MB";
		String chunkedFileName = baseFileName + "_Chunked";
		File bigFile = null;
		RandomAccessFile raf = null;

		// Make sure files don't exist already
		try {
			provider.delete(drive, baseFileName);
			provider.delete(drive, chunkedFileName);
		}
		catch (Exception e) {
			// ignore failures
		}

		assertFalse(provider.exists(drive, baseFileName));

		try {
			// See how long it takes to upload using TransferManager
			
			assertFalse(provider.exists(drive, baseFileName));
			bigFile = createFileOfSize(baseFileName, fileSize);
			raf = new RandomAccessFile(bigFile, "rw");
			long startTime = System.currentTimeMillis();
			provider.upload(drive, baseFileName, bigFile);
			logger.info("Upload of " + baseFileName + " took " +
						(System.currentTimeMillis() - startTime) + " ms.");
			assertTrue(provider.exists(drive, baseFileName));

			// See how long it takes to upload using chunks
			
			long uploadTimeNoChunks = 0;
			long uploadTimePreStart = System.currentTimeMillis();
			String uploadId = provider.uploadPartStart(drive, chunkedFileName);
			uploadTimeNoChunks += (System.currentTimeMillis() - uploadTimePreStart);
			byte[] bytes = new byte[chunkSize];
			long startByte = 0;
			int partNumber = 1;

			for (int i = 0; i < numChunks; i++) {
				raf.seek(startByte);
				int bytesRead = raf.read(bytes);
				long uploadPreChunk = System.currentTimeMillis();
				provider.uploadPart(drive, chunkedFileName, bytes, partNumber);
				uploadTimeNoChunks += (System.currentTimeMillis() - uploadPreChunk);
				partNumber++;
				startByte += chunkSize;
			}
			long uploadTimePreComplete = System.currentTimeMillis();
			provider.uploadPartComplete(drive, chunkedFileName, uploadId);
			uploadTimeNoChunks += (System.currentTimeMillis() - uploadTimePreComplete);
			logger.info("Upload of " + chunkedFileName + " using chunks took " +
						uploadTimeNoChunks + " ms.");
			assertTrue(provider.exists(drive, chunkedFileName));

			// clean up
			provider.delete(drive, chunkedFileName);
			assertFalse(provider.exists(drive, chunkedFileName));
		} catch (AmazonServiceException ase) {
			printAmazonServiceException(ase);
		} catch (AmazonClientException ace) {
			printAmazonClientException(ace);
		} finally {
			// clean up
			if (bigFile != null) {
				bigFile.delete();
				bigFile = null;
			}
			if (raf != null) {
				raf.close();
			}
			provider.delete(drive, baseFileName);
			provider.delete(drive, chunkedFileName);
		}
	}

	@Test
	@Tag("integration")
	@Order(350)
	public void testDownload() throws IOException {
		Drive drive = buildDrive();

		try {
			File downloaded = provider.download(drive, PREEXISTING_FILE);
			assertTrue(downloaded.exists());
			assertTrue(downloaded.canRead());
			assertTrue(downloaded.length() > 10);
		} catch (AmazonServiceException ase) {
			printAmazonServiceException(ase);
		} catch (AmazonClientException ace) {
			printAmazonClientException(ace);
		}
	}


	@Test
	@Tag("integration")
	@Order(370)
	public void testDownloadBytes() throws IOException {
		Drive drive = buildDrive();

		try {
			int numBytes = 5;
			provider.downloadPartStart(drive, PREEXISTING_FILE);
			byte[] downloaded = provider.downloadBytes(drive, PREEXISTING_FILE, 0, numBytes);
			assertEquals(numBytes, downloaded.length);
			assertEquals("Integ", new String(downloaded));
			
			numBytes = 4;
			byte[] downloaded2 = provider.downloadBytes(drive, PREEXISTING_FILE, 3, numBytes);
			assertEquals(numBytes, downloaded2.length);
			assertEquals("egra", new String(downloaded2));

		} catch (AmazonServiceException ase) {
			printAmazonServiceException(ase);
		} catch (AmazonClientException ace) {
			printAmazonClientException(ace);
		}
	}

	@Test
	@Tag("integration")
	@Order(371)
	public void testDownloadBytesFromEmptyFile() throws IOException {
		Drive drive = buildDrive();

		try {
			int numBytes = 5;
			provider.downloadPartStart(drive, EMPTY_FILE_S3);
			byte[] downloaded = provider.downloadBytes(drive, EMPTY_FILE_S3, 0, numBytes);
			assertEquals(0, downloaded.length);
		} catch (AmazonServiceException ase) {
			printAmazonServiceException(ase);
		} catch (AmazonClientException ace) {
			printAmazonClientException(ace);
		}
	}


	@Test
	@Tag("integration")
	@Order(400)
	public void testRenameUploadedFile() throws IOException {
		Drive drive = buildDrive();

		File file = FileUtil.getResourceFile("sampleFolder/fileOne.txt");
		String fileName = file.getName();
		String mvFileName = fileName + "mv";

		if (!provider.exists(drive, fileName)) {
			provider.upload(drive, fileName, file);
			assertTrue(provider.exists(drive, fileName), fileName + " not found");
		}
		if (provider.exists(drive, mvFileName)) {
			provider.delete(drive, mvFileName);
		}

		provider.rename(drive, fileName, mvFileName);
		assertFalse(provider.exists(drive, fileName), fileName + " was not renamed");
		assertTrue(provider.exists(drive, mvFileName), mvFileName + " not found");

		Assertions.assertThrows(FileAlreadyExistsException.class, () -> {
			provider.rename(drive, PREEXISTING_FILE, mvFileName);
		});

		provider.delete(drive, mvFileName);
		assertFalse(provider.exists(drive, mvFileName), mvFileName + " not found");
	}

	@Test
	@Tag("integration")
	@Order(410)
	public void testRenameFileFailsBecauseFileAlreadyExists() throws IOException {
		Drive drive = buildDrive();

		File sampleFile1 = createSampleFile();
		String sampleFileName1 = sampleFile1.getName();
		provider.upload(drive, sampleFileName1, sampleFile1);

		File sampleFile2 = createSampleFile();
		String sampleFileName2 = sampleFile2.getName();
		provider.upload(drive, sampleFileName2, sampleFile2);

		Assertions.assertThrows(FileAlreadyExistsException.class, () ->
				provider.rename(drive, sampleFileName1, sampleFileName2)
		);

		provider.delete(drive, sampleFileName1);
		provider.delete(drive, sampleFileName2);

		assertFalse(provider.exists(drive, sampleFileName1));
		assertFalse(provider.exists(drive, sampleFileName2));
	}

	@Test
	@Tag("integration")
	@Order(420)
	public void testRenameDirFailsBecauseDirAlreadyExists() throws IOException {
		Drive drive = buildDrive();

		File sampleFile1 = new File("src/test/resources/quotesFolder");
		String sampleFileName1 = sampleFile1.getName();
		provider.uploadDirectory(drive, sampleFileName1, sampleFile1);

		File sampleFile2 = createSampleFile();
		String sampleFileName2 = sampleFile2.getName();
		provider.upload(drive, sampleFileName2, sampleFile2);

		Assertions.assertThrows(FileAlreadyExistsException.class, () ->
				provider.rename(drive, sampleFileName1, sampleFileName2)
		);

		provider.delete(drive, sampleFileName1);
		provider.delete(drive, sampleFileName2);

		assertFalse(provider.exists(drive, sampleFileName1));
		assertFalse(provider.exists(drive, sampleFileName2));
	}

	@Test
	@Tag("integration")
	@Order(430)
	public void testRenameDir() throws IOException {
		Drive drive = buildDrive();

		String unusedDirName = "unusedDirName/";
		try {	// make sure the target directory doesn't exist
			if (provider.exists(drive, unusedDirName)) {
				provider.delete(drive, unusedDirName);
			}
		}
		catch (Exception e) {
			// do nothing
		}

		File quotesFolder = new File("src/test/resources/quotesFolder");
		String quotesFolderName = quotesFolder.getName();
		provider.uploadDirectory(drive, quotesFolderName, quotesFolder);

		provider.rename(drive, quotesFolderName, unusedDirName);
		assertFalse(provider.exists(drive, quotesFolderName));

		provider.delete(drive, unusedDirName);
		assertFalse(provider.exists(drive, unusedDirName));
	}

	@Test
	@Tag("integration")
	@Order(450)
	public void testCopyInDir() throws IOException {
		Drive drive = buildDrive();

		try {
			String fileOnePath = "sampleFolder/fileOne.txt";
			File file = FileUtil.getResourceFile(fileOnePath);

			if (!provider.exists(drive, fileOnePath)) {
				provider.upload(drive, fileOnePath, file);
			}

			assertTrue(provider.exists(drive, fileOnePath), fileOnePath + " not found");

			String destPath = fileOnePath + "cp";
			provider.copy(drive, fileOnePath, destPath);

			assertTrue(provider.exists(drive, fileOnePath),
					fileOnePath + " should still be present after copy");
			assertTrue(provider.exists(drive, destPath),
					destPath + " should be present after copy");

			// clean up
			provider.delete(drive, fileOnePath);
			provider.delete(drive, destPath);
		} catch (AmazonClientException e) {
			printAmazonClientException(e);
		}
	}

	@Test
	@Tag("integration")
	@Order(460)
	public void testCopyAcrossDirs() throws IOException {
		Drive drive = buildDrive();

		try {
			String fileOnePath = "sampleFolder/fileOne.txt";
			File file = FileUtil.getResourceFile(fileOnePath);

			if (!provider.exists(drive, fileOnePath)) {
				provider.upload(drive, fileOnePath, file);
			}

			assertTrue(provider.exists(drive, fileOnePath),
					fileOnePath + " should still be present after copy");

			String destPath = PathProcessor.addLastSlash(PREEXISTING_DIR) + fileOnePath + "cp";
			provider.copy(drive, fileOnePath, destPath);

			assertTrue(provider.exists(drive, fileOnePath),
					fileOnePath + " should still be present after copy");
			assertTrue(provider.exists(drive, destPath),
					destPath + " should be present after copy");

			// clean up
			provider.delete(drive, fileOnePath);
			provider.delete(drive, destPath);
		} catch (AmazonClientException e) {
			printAmazonClientException(e);
		}
	}

	@Disabled("Storage class problems")
	@Test
	@Tag("integration")
	@Order(470)
	public void testCopyDir() throws IOException {
		Drive drive = buildDrive();

		try {
			String fileOnePath = "sampleFolder/fileOne.txt";
			File file = FileUtil.getResourceFile(fileOnePath);

			if (!provider.exists(drive, fileOnePath)) {
				provider.upload(drive, fileOnePath, file);
			}

			String sourceDir = "sampleFolder/";

			String destPath = sourceDir + "cp/";
			provider.copy(drive, sourceDir, destPath);

			assertTrue(provider.exists(drive, sourceDir),
					sourceDir + " should still be present after copy");
			assertTrue(provider.exists(drive, "cp/fileOne.txt"),
					destPath + " should be present after copy");

			// clean up
			provider.delete(drive, sourceDir);
			provider.delete(drive, destPath);
		} catch (AmazonClientException e) {
			printAmazonClientException(e);
		}
	}

	@Test
	@Tag("integration")
	@Order(480)
	public void testMoveAcrossDirs() throws IOException {
		Drive drive = buildDrive();

		try {
			assertTrue(provider.exists(drive, PREEXISTING_FILE),
					PREEXISTING_FILE + " not found");
			logger.info("Found " + PREEXISTING_FILE + " before move.");

			TransferSpec spec = new TransferSpec();
			spec.setSourcePath(PREEXISTING_FILE);
			String destPath = PathProcessor.addLastSlash(PREEXISTING_DIR) + PREEXISTING_FILE + "mv";
			spec.setDestPath(destPath);
			provider.move(drive, spec);

			assertFalse(provider.exists(drive, PREEXISTING_FILE),
					PREEXISTING_FILE + " should not still be present after move");
			assertTrue(provider.exists(drive, destPath),
					destPath + " should be present after move");
			logger.info(PREEXISTING_FILE + " moved to " + destPath);

			// clean up
			spec = new TransferSpec();
			spec.setSourcePath(destPath);
			spec.setDestPath(PREEXISTING_FILE);
			provider.move(drive, spec);

			assertTrue(provider.exists(drive, PREEXISTING_FILE),
					PREEXISTING_FILE + " should be present after 2nd move");
			assertFalse(provider.exists(drive, destPath),
					destPath + " should not still be present after 2nd move");
			logger.info(destPath + " moved to " + PREEXISTING_FILE);
		} catch (AmazonClientException e) {
			printAmazonClientException(e);
		}
	}


	@Test
	@Tag("integration")
	@Order(500)
	public void testDelete() throws IOException {
		Drive drive = buildDrive();

		try {
			File file = FileUtil.getResourceFile("sampleFolder/fileOne.txt");
			String fileName = file.getName();
			provider.upload(drive, fileName, file);

			assertTrue(provider.exists(drive, fileName), fileName + " not found");

			provider.delete(drive, fileName);
			assertFalse(provider.exists(drive, fileName), fileName + " not found");
		} catch (AmazonServiceException ase) {
			printAmazonServiceException(ase);
		} catch (AmazonClientException ace) {
			printAmazonClientException(ace);
		}

	}

	@Test
	@Tag("integration")
	@Order(510)
	public void testDoubleDelete() throws IOException {
		Drive drive = buildDrive();

		try {
			provider.delete(drive, "fileThatDoesntExist.txt");
		} catch (AmazonServiceException ase) {
			printAmazonServiceException(ase);
		} catch (AmazonClientException ace) {
			printAmazonClientException(ace);
		}

	}

	@Test
	@Tag("integration")
	@Order(510)
	public void testMkdir() throws IOException {
		Drive drive = buildDrive();
		String newDir =
				DayOfWeek.from(LocalDate.now()).toString() + "_" +
						System.currentTimeMillis() + "/";

		provider.mkdir(drive, newDir);
		assertTrue(provider.exists(drive, newDir), newDir + " not found");
		logger.info("Found newly created directory " + newDir);

		//cleanup
		provider.delete(drive, newDir);
		assertFalse(provider.exists(drive, newDir), newDir + " is found, but should have deleted");
	}

	@Test
	@Tag("integration")
	@Order(520)
	public void testMkdirAlreadyExistingThrowsException() throws IOException {
		Drive drive = buildDrive();
		String newDir =
				DayOfWeek.from(LocalDate.now()).toString() + "_" +
						System.currentTimeMillis() + "/";

		provider.mkdir(drive, newDir);
		assertTrue(provider.exists(drive, newDir), newDir + " not found");
		logger.info("Found newly created directory " + newDir);

		// A second attempt to create the same directory should fail
		assertThrows(FileAlreadyExistsException.class, () ->
				provider.mkdir(drive, newDir)
		);

		//cleanup
		provider.delete(drive, newDir);
		assertFalse(provider.exists(drive, newDir), newDir + " is found, but should have deleted");
	}

	@Test
	@Tag("integration")
	@Order(530)
	public void testMkdirSubdir() throws IOException {
		Drive drive = buildDrive();
		String newDir = "csv-dir/" +
				DayOfWeek.from(LocalDate.now()).toString() + "_" +
				System.currentTimeMillis() + "/";

		provider.mkdir(drive, newDir);
		assertTrue(provider.exists(drive, newDir), newDir + " not found");
		logger.info("Found newly created directory " + newDir);

		provider.delete(drive, newDir);
		assertFalse(provider.exists(drive, newDir), newDir + " not found");
	}
	

    /**
     * Creates a temporary file with text data to demonstrate
     *  uploading a file to Amazon S3
     * @return A new file with text data.
     * @throws IOException
     */
//	File createSampleDirectory() throws IOException {
//		Path of = Path.of("aws-java-sdk-");
//		return Files.createDirectory(of);
//	}

	@Disabled
	@Test
	@Tag("integration")
	public void testZipExtract() throws Exception {
		Drive drive = buildDrive();

		//the files inside /folder2718 are:
		// - /testFolder/doc1.docx and
		// - /testFolder/text.txt
		String decompressedFilePath = "Uploads/folder2718";
		String compressedFilePath = decompressedFilePath + ".zip";

		FileLocationSpec locationSpec = new FileLocationSpec();
		locationSpec.setDriveId(1);
		locationSpec.setPath(compressedFilePath);

		driveService.extractFile(locationSpec, request, response);

		DriveQuery query = new DriveQuery();
		query.setDriveId(drive.getDriveId());
		query.setSearchPattern(decompressedFilePath + "/testFolder/text.txt");

		assertEquals(1, provider.find(drive, query).size());

		provider.deleteDirectory(drive, decompressedFilePath + "/");

		assertEquals(0, provider.find(drive, query).size());
	}

	@Disabled
	@Test
	@Tag("integration")
	public void testGzExtract() throws Exception {
		Drive drive = buildDrive();

		String decompressedFilePath = "THURSDAY_1607602496028/text.txt";
		String compressedFilePath = decompressedFilePath + ".gz";

		FileLocationSpec locationSpec = new FileLocationSpec();
		locationSpec.setDriveId(1);
		locationSpec.setPath(compressedFilePath);

		driveService.extractFile(locationSpec, request, response);

		DriveQuery query = new DriveQuery();
		query.setDriveId(drive.getDriveId());
		query.setSearchPattern(decompressedFilePath);

		assertEquals(1, provider.find(drive, query).size());

		provider.delete(drive, decompressedFilePath);

		assertEquals(0, provider.find(drive, query).size());
	}

	@Test
	@Tag("integration")
	public void testUpdateStorageClass() throws Exception {
		Drive drive = buildDrive();

		File sampleFile = createSampleFile();
		String filePath = sampleFile.getName();
		provider.upload(drive, filePath, sampleFile);

		final StorageClass STANDARD_IA_CLASS = new StorageClass(STANDARD_IA, "Standard-IA", false);
		provider.updateStorageClass(buildDrive(), filePath, STANDARD_IA_CLASS);

		DriveItem driveItem = provider.getDriveItem(drive, filePath);
		String storageClassName = driveItem.getStorageClass().getClassName();

		assertEquals(storageClassName, STANDARD_IA);

		provider.delete(drive, filePath);
		assertFalse(provider.exists(drive, filePath));
	}

	//this test takes 10~ seconds, but is an important edge case
	//the bug is where a file with a new storage class is moved, then loses its original storage class
	//performs an upload, storage class change, transfer, and delete
	@Test
	@Disabled("See comments above")
	@Tag("integration")
	public void testUpdateStorageClassAndMove() throws Exception {
		Drive drive = buildDrive();

		File sampleFile = createSampleFile();
		String filePath = sampleFile.getName();
		provider.upload(drive, filePath, sampleFile);

		final StorageClass STANDARD_IA_CLASS = new StorageClass(STANDARD_IA, "Standard-IA", false);
		provider.updateStorageClass(buildDrive(), filePath, STANDARD_IA_CLASS);

		DriveItem driveItem = provider.getDriveItem(drive, filePath);
		String storageClassName = driveItem.getStorageClass().getClassName();

		assertEquals(storageClassName, STANDARD_IA);

		TransferSpec spec = new TransferSpec();
		spec.setSourceDriveId(drive.getDriveId());
		spec.setSourcePath(filePath);

		spec.setDestDriveId(drive.getDriveId());
		String destPath = "csv-dir/" + filePath;
		spec.setDestPath(destPath);

		provider.move(drive, spec);

		DriveItem movedItem = provider.getDriveItem(drive, destPath);
		assertEquals(STANDARD_IA_CLASS, movedItem.getStorageClass());
		provider.delete(drive, destPath);
		assertFalse(provider.exists(drive, destPath));
	}

	@Test
	@Tag("integration")
	public void testRestore() throws Exception {
		Drive drive = buildDrive();

		File sampleFile = createSampleFile();
		String filePath = sampleFile.getName();
		provider.upload(drive, filePath, sampleFile);

		provider.updateStorageClass(buildDrive(), filePath, new StorageClass(GLACIER, "Glacier", false));

		DriveItem driveItem = provider.getDriveItem(drive, filePath);
		String storageClassName = driveItem.getStorageClass().getClassName();
		assertEquals(storageClassName, GLACIER);

		//file is uploaded successfully AND set to Glacier; now attempt to restore

		provider.restore(drive, filePath, 2);

		provider.delete(drive, filePath);
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
			assertEquals(src, tl, "Line " + i + "differed:" +
							StringUtils.difference(src, tl));
		}
	}

	@Test
	@Tag("integration")
	public void testGetTopLinesTooFew() throws Exception {
		File sourceFile = FileUtil.getResourceFile("csvFiles/medications.csv");
		List<String> sourceContents =
			Files.readAllLines(Paths.get(sourceFile.getCanonicalPath()));

		Drive drive = buildDrive();
		int numLines = 1000; // asking for more lines than are in the file

		BasicFile topLinesFile =
				provider.getTopLines(drive, "csv-dir/medications.csv", numLines);
		String tempFile = topLinesFile.getCanonicalPath();
		logger.info("Top lines in file: " + tempFile);
		List<String> tlContents = Files.readAllLines(Paths.get(tempFile));
		assertNotNull(tlContents);
		int numTopLines = tlContents.size();
		assertEquals(428, numTopLines);

		for (int i = 0; i < numTopLines; i++) {
			String src = sourceContents.get(i);
			String tl = tlContents.get(i);
			assertEquals(src, tl, "Line " + i + "differed:" +
							StringUtils.difference(src, tl));
		}
	}
	
	
	@Test
	@Tag("integration")
	public void testGetInputStream() throws IOException {
		Drive drive = buildDrive();

		int numBytes = 5;
		try (InputStream inputStream = provider.getInputStream(drive, PREEXISTING_FILE)) {
			byte[] bytes = new byte[numBytes];
			inputStream.read(bytes);
			assertEquals(numBytes, bytes.length);
			assertEquals("Integ", new String(bytes));
			
			inputStream.read(bytes);
			assertEquals(numBytes, bytes.length);
			assertEquals("ratio", new String(bytes));
			
			inputStream.read(bytes, 1, 2);
			assertEquals(numBytes, bytes.length);
			assertEquals("rn io", new String(bytes));
		}
	}

	////////// helper methods below

	private void printAmazonClientException(AmazonClientException ace) {
		logger.warning("Caught an AmazonClientException, which means the client encountered "
				+ "a serious internal problem while trying to communicate with S3, "
				+ "such as not being able to access the network.");
		logger.warning("Error Message: " + ace.getMessage());
	}


	private void printAmazonServiceException(AmazonServiceException ase) {
		logger.warning("Caught an AmazonServiceException, which means your request made it "
				+ "to Amazon S3, but was rejected with an error response for some reason.");
		logger.warning("Error Message:    " + ase.getMessage());
		logger.warning("HTTP Status Code: " + ase.getStatusCode());
		logger.warning("AWS Error Code:   " + ase.getErrorCode());
		logger.warning("Error Type:       " + ase.getErrorType());
		logger.warning("Request ID:       " + ase.getRequestId());
	}

    /**
     * Creates a temporary file with text data to demonstrate
     *  uploading a file to Amazon S3
     * @return A new file with text data.
     * @throws IOException
     */
    File createSampleFile() throws IOException {
            File file = File.createTempFile(FileUtil.MDACA_PREFIX + "testAWS_", ".txt");
            file.deleteOnExit();

            Writer writer = new OutputStreamWriter(new FileOutputStream(file));
            writer.write("Sample file created on " + new Date() + "\n");
            writer.close();
            return file;
    }

	private File create5GBFile(String fileName) throws IOException {
		long fileSize = 5L * 1024L * 1024L * 1024L + 1L;
		File file = createFileOfSize(fileName, fileSize);
		return file;
	}

	private File createFileOfSize(String fileName, long fileSize) throws IOException, FileNotFoundException {
		File file = new File(fileName);
		file.createNewFile();

		RandomAccessFile raf = new RandomAccessFile(file, "rw");
		// AWS S3 has a 5GB limit for some operations, so we make it slightly larger
		raf.setLength(fileSize);
		raf.close();
		return file;
	}

	@Override
	protected AbstractStorageProvider getProvider() {
		return new AWSS3StorageProvider();
	}


}
