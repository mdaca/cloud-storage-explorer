package com.spinsys.mdaca.storage.explorer.provider;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.file.FileAlreadyExistsException;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.util.Date;
import java.util.List;
import java.util.logging.Logger;

import com.spinsys.mdaca.storage.explorer.io.FileUtil;
import com.spinsys.mdaca.storage.explorer.model.enumeration.DriveType;
import com.spinsys.mdaca.storage.explorer.persistence.Drive;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import com.spinsys.mdaca.storage.explorer.model.DriveItem;
import com.spinsys.mdaca.storage.explorer.model.DriveQuery;

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

/**
 * Contains integration tests for SMBStorageProvider
 * 
 * These tests are highly dependent on files and directories
 * being in certain places on the drive, on certain environment
 * variables (see initializeTestDrive()), and on test ordering
 * (@Order).
 */
// TODO - reduce dependencies, further automate initial state restoration

@TestMethodOrder(OrderAnnotation.class)
public class SMBStorageProviderIT {
	
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
            Logger.getLogger("com.spinsys.mdaca.storage.explorer.provider.SMBStorageProviderIT");

	/** We'll store this file between tests, so we can rename it,
	 * delete it, etc.  If we're lucky, we can sequence our tests
	 * such that the SMB drive ends up in the same state it was
	 * in the beginning by deleting this uploaded file at the end.
	 */
	private static String uploadedFileName = null;

	SMBStorageProvider provider = new SMBStorageProvider();
	

	public static Drive initializeTestDrive() {
		Drive drive = new Drive();
		drive.setDisplayName("SMB_Share");
		drive.setDriveType(DriveType.SMB);
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
		return drive;
	}

	@Test
	@Tag("integration")
	@Order(100)
	public void testExistsFile() throws IOException {
		Drive drive = initializeTestDrive();

		assertTrue(provider.exists(drive, PREEXISTING_FILE),
				PREEXISTING_FILE + " should exist on " + drive);
	}

	@Test
	@Tag("integration")
	@Order(110)
	public void testExistsDir() throws IOException {
		Drive drive = initializeTestDrive();

		assertTrue(provider.exists(drive, PREEXISTING_DIR),
				PREEXISTING_DIR + " should exist on " + drive);
	}

	@Test
	@Tag("integration")
	@Order(120)
	public void testExistsSubdirFile() throws IOException {
		Drive drive = initializeTestDrive();

		assertTrue(provider.exists(drive, PREEXISTING_SUBDIR_FILE),
				PREEXISTING_SUBDIR_FILE + " should exist on " + drive);
	}

	@Test
	@Tag("integration")
	@Order(130)
	public void testExistsFalse() throws IOException {
		Drive drive = initializeTestDrive();

		String path = "ybgfuyc76";
		assertFalse(provider.exists(drive, path),
				path + " should not exist on " + drive);
	}

	@Test
	@Tag("integration")
	public void testDownloadFileNormal() throws IOException {
		Drive drive = initializeTestDrive();

		File download = provider.download(drive, PREEXISTING_FILE);
		assertTrue(provider.exists(drive, PREEXISTING_FILE),
				PREEXISTING_FILE + " should have downloaded");
		// clean up
		download.delete();
		assertFalse(download.exists());
	}

	@Test
	@Tag("integration")
	public void testDownloadFileMissing() throws IOException {
		Drive drive = initializeTestDrive();

		DriveQuery query = new DriveQuery();
		query.setDriveId(drive.getDriveId());
		String path = "ifyufqvf";
		File download = provider.download(drive, path);
		assertNull(download, "Attempt to download non-existent " + path +
				" should have returned null.");
	}
	
	/**
	 * Testing an attempt to find a file that doesn't exist.
	 * @throws IOException
	 */
	@Test
	@Tag("integration")
	public void testFindGarbageRecursive() throws IOException {
		Drive drive = initializeTestDrive();
		DriveQuery query = new DriveQuery();
		query.setDriveId(drive.getDriveId());
		query.setRecursive(true);
		query.setSearchPattern(".*yurwufru.*");
		query.setStartPath("csv-dir"); // start at csv-dir
		
		List<DriveItem> items = provider.find(drive, query);
		int size = items.size();
		logger.info("Found " + size + " results using the query: " + query);
		assertEquals(0, size, "There should be no disk objects matching .*yurwufru.*");
	}


	@Test
	@Tag("integration")
	@Order(220)
	public void testFindFromRootUsingRegex() throws IOException {
		Drive drive = initializeTestDrive();

		DriveQuery query = new DriveQuery();
		query.setDriveId(drive.getDriveId());
		query.setRecursive(true);
		query.setSearchPattern(".*txt.*");
//			query.setSearchPattern(""); // get everything
		query.setStartPath(null); // start at the root
//			query.setStartPath("csv-dir"); // start at csv-dir
		
		List<DriveItem> items = provider.find(drive, query);
		int size = items.size();
		logger.info("Found " + size + " results using the query: " + query);
		assertTrue(size > 0);
		assertTrue(items.get(0).getPath().contains("txt"));
	}

	@Test
	@Tag("integration")
	@Order(230)
	public void testFindAll() throws IOException {
		Drive drive = initializeTestDrive();

		DriveQuery query = new DriveQuery();
		query.setDriveId(drive.getDriveId());
		query.setRecursive(true);
		query.setSearchPattern(""); // get everything
//			query.setStartPath("csv-dir"); // start at csv-dir
		
		List<DriveItem> items = provider.find(drive, query);
		int size = items.size();
		logger.info("Found " + size + " results using the query: " + query);
		assertTrue(size > 0);

		query.setSearchPattern(".*txt.*");
		query.setStartPath("csv-dir"); // start at csv-dir
		
		List<DriveItem> itemsK = provider.find(drive, query);
		int sizeK = itemsK.size();
		logger.info("Found " + sizeK + " results using the query: " + query);
		assertTrue(sizeK > 0);
		assertTrue(itemsK.get(0).getPath().contains("txt"));
		assertTrue(sizeK < size);
	}

	@Test
	@Tag("integration")
	@Order(240)
	public void testFromDir() throws IOException {
		Drive drive = initializeTestDrive();

		DriveQuery query = new DriveQuery();
		query.setDriveId(drive.getDriveId());
		query.setRecursive(true);
		query.setSearchPattern(""); // get everything
		
		List<DriveItem> items = provider.find(drive, query);
		int size = items.size();
		logger.info("Found " + size + " results using the query: " + query);
		assertTrue(size > 0);

		query.setStartPath("csv-dir"); // start at csv-dir		
		List<DriveItem> itemsCSV = provider.find(drive, query);
		int sizeCSV = itemsCSV.size();
		logger.info("Found " + sizeCSV + " results using the query: " + query);
		assertTrue(sizeCSV > 0);
		assertTrue(itemsCSV.get(0).getPath().contains("csv"));
		assertTrue(sizeCSV < size);
	}

	@Test
	@Tag("integration")
	@Order(500)
	public void testUpload() throws IOException {
		Drive drive = initializeTestDrive();
		
		File sampleFile = createSampleFile();
		String fileName = sampleFile.getName();
		provider.upload(drive, fileName, sampleFile);

		assertTrue(provider.exists(drive, fileName));

		provider.delete(drive, fileName);
		assertFalse(provider.exists(drive, fileName));
	}

	@Test
	@Tag("integration")
	@Order(510)
	public void testUploadExistingThrowsException() throws IOException {
		Drive drive = initializeTestDrive();

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

		Drive drive = initializeTestDrive();
		String folderName = "quotesFolder";
		provider.uploadDirectory(drive, folderName, dir);

		DriveQuery query = new DriveQuery("quotesFolder/quotes");
		List<DriveItem> driveItems = provider.find(drive, query);

		assertEquals(2, driveItems.size());

		provider.deleteDirectory(drive, folderName);

		assertFalse(provider.exists(drive, folderName), (folderName + " should be deleted, but still exists"));
	}

	@Test
	@Tag("integration")
	@Order(600)
	public void testRename() throws IOException {
		Drive drive = initializeTestDrive();

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

		Assertions.assertThrows(FileAlreadyExistsException.class, () ->
				provider.rename(drive, PREEXISTING_FILE, mvFileName)
		);
	
		// Update the stored file name so we can delete it later
		uploadedFileName = mvFileName;
	}

	@Test
	@Tag("integration")
	@Order(610)
	public void testRenameSubdirFile() throws IOException {
		Drive drive = initializeTestDrive();

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
	@Order(620)
	public void testRenameSubdirFileSlash() throws IOException {
		Drive drive = initializeTestDrive();

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

	@Disabled
	@Test
	@Tag("integration")
	@Order(6660)
	public void testRenameSubdirFileSlashProblem() throws IOException {
		Drive drive = initializeTestDrive();

		assertTrue(provider.exists(drive, PREEXISTING_SUBDIR_FILE_SLASH),
				PREEXISTING_SUBDIR_FILE_SLASH + " not found");
		logger.info("Found " + PREEXISTING_SUBDIR_FILE_SLASH + " before rename.");

		String mvFileName = PREEXISTING_SUBDIR_FILE_SLASH + "mv";
		provider.renameWithProblem(drive, PREEXISTING_SUBDIR_FILE_SLASH, mvFileName);
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

	@Test
	@Tag("integration")
	@Order(710)
	public void testCopySubdirFile() throws IOException {
		Drive drive = initializeTestDrive();

		assertTrue(provider.exists(drive, PREEXISTING_SUBDIR_FILE),
				PREEXISTING_SUBDIR_FILE + " not found");
		logger.info("Found " + PREEXISTING_SUBDIR_FILE + " before copy.");

		String cpFileName = PREEXISTING_SUBDIR_FILE + "cp";
		provider.copy(drive, PREEXISTING_SUBDIR_FILE, cpFileName);
		assertTrue(provider.exists(drive, PREEXISTING_SUBDIR_FILE),
				PREEXISTING_SUBDIR_FILE + " was improperly removed");

		assertTrue(provider.exists(drive, cpFileName),
					cpFileName + " not found");
		logger.info("Found " + cpFileName + " after copy.");
		
		// Restore the original state
		provider.delete(drive, cpFileName);
	}


	/**
	 * Typical case - new directory, no name conflict
	 * @throws IOException
	 */
	@Test
	@Tag("integration")
	@Order(810)
	public void testMkdir() throws IOException {
		Drive drive = initializeTestDrive();
		String newDir =
				DayOfWeek.from(LocalDate.now()).toString() + "_" +
						System.currentTimeMillis();

		provider.mkdir(drive, newDir);
		assertTrue(provider.exists(drive, newDir), newDir + " not found");
		logger.info("Found newly created directory " + newDir);

		//cleanup
		provider.deleteDirectory(drive, newDir);
		assertFalse(provider.exists(drive, newDir), (newDir + " should be deleted, but still exists"));
	}

	@Test
	@Tag("integration")
	@Order(820)
	public void testMkdirExistingThrowsExcetpion() throws IOException {
		Drive drive = initializeTestDrive();
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
		Drive drive = initializeTestDrive();
		String leafDirName = DayOfWeek.from(LocalDate.now()).toString() +
				"_" + System.currentTimeMillis();
		String newDir = "csv-dir\\" + leafDirName;

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
		Drive drive = initializeTestDrive();
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
	@Order(1100)
	public void testByteUpload() throws IOException {
		SMBStorageProvider uploadProvider = new SMBStorageProvider();
		SMBStorageProvider downloadProvider = new SMBStorageProvider();
		Drive drive = initializeTestDrive();
		// 10K file - 1K of 0s, followed by 1K of 1s, 1K of 2s, ...
		String readFilePath = "csv-dir/numbers.txt";
		String writeFilePath = "newFile.txt";

		try {

			if (!provider.exists(drive, readFilePath)) {
				File numbersFile = FileUtil.getResourceFile("numbers.txt");
				provider.upload(drive, readFilePath, numbersFile);
			}

			if (provider.exists(drive, writeFilePath)) {
				provider.delete(drive, writeFilePath);
			}

			uploadProvider.uploadPartStart(drive, writeFilePath);
			downloadProvider.downloadPartStart(drive, readFilePath);

			int chunkSize = 5120;
			byte[] bytes = downloadProvider.downloadBytes(drive, readFilePath, 0, chunkSize);
			uploadProvider.uploadPart(drive, writeFilePath, bytes, 0);

			String s = new String(bytes);
			assertEquals(chunkSize, s.length());
			assertEquals(s.charAt(500), '0');
			assertEquals(s.charAt(999), '0');
			assertEquals(s.charAt(1000), '1');
			assertEquals(s.charAt(1500), '1');
			assertEquals(s.charAt(2500), '2');
			assertEquals(s.charAt(3500), '3');
			assertEquals(s.charAt(4500), '4');
			assertEquals(s.charAt(4999), '4');
			assertEquals(s.charAt(5000), '5');
			assertEquals(s.charAt(chunkSize - 1), '5');

			bytes = downloadProvider.downloadBytes(drive, readFilePath, chunkSize, chunkSize * 2);
			s = new String(bytes);

			uploadProvider.uploadPart(drive, writeFilePath, bytes, 1);
			assertEquals(10_000 - chunkSize, s.length());
			assertEquals(s.charAt(500), '5');
			assertEquals(s.charAt(1500), '6');
			assertEquals(s.charAt(2500), '7');
			assertEquals(s.charAt(3500), '8');
			assertEquals(s.charAt(4500), '9');
		} finally {
			uploadProvider.uploadPartComplete(drive, readFilePath, "");
			downloadProvider.downloadComplete(drive, readFilePath);
		}
		provider.delete(drive, readFilePath);
		provider.delete(drive, writeFilePath);
	}

	@Test
	@Tag("integration")
	public void testDownloadBytesEmptyFile() throws IOException {
		SMBStorageProvider provider = new SMBStorageProvider();
		Drive drive = initializeTestDrive();
		String emptyFilePath = "sizedFiles/emptyFile.txt";
		File emptyFile = FileUtil.getResourceFile(emptyFilePath);
		
		try {
			provider.upload(drive, emptyFilePath, emptyFile);
		}
		catch (IOException e) {
			// ignore - file might already exist
		}
		assertTrue(provider.exists(drive, emptyFilePath));

		provider.downloadPartStart(drive, emptyFilePath);
		byte[] bytes = provider.downloadBytes(drive, emptyFilePath, 0, 5120);
		provider.downloadComplete(drive, emptyFilePath);
		assertEquals(0, bytes.length);
		
		// clean up
		provider.delete(drive, emptyFilePath);
	}
	
	@Test
	@Tag("integration")
	public void testDownloadBytesSmallFile() throws IOException {
		SMBStorageProvider provider = new SMBStorageProvider();
		Drive drive = initializeTestDrive();
		String path = "sizedFiles/stateCodes_u1kb.csv";
		File file = FileUtil.getResourceFile(path);
		
		try {
			provider.upload(drive, path, file);
		}
		catch (IOException e) {
			// ignore - file might already exist
		}
		assertTrue(provider.exists(drive, path));

		provider.downloadPartStart(drive, path);
		byte[] bytes = provider.downloadBytes(drive, path, 0, 5120);
		provider.downloadComplete(drive, path);
		assertEquals(85, bytes.length);
		
		// clean up
		provider.delete(drive, path);
	}
	
	@Test
	@Tag("integration")
	public void testDownloadBytesLargeFile() throws IOException {
		SMBStorageProvider provider = new SMBStorageProvider();
		Drive drive = initializeTestDrive();
		String filePath = "sizedFiles/MDACA_CloudStorage_UserGuide_1.0_o2mb.docx";
		File file = FileUtil.getResourceFile(filePath);
		
		try {
			provider.upload(drive, filePath, file);
		}
		catch (IOException e) {
			// ignore - file might already exist
		}
		assertTrue(provider.exists(drive, filePath));

		provider.downloadPartStart(drive, filePath);
		byte[] bytes = provider.downloadBytes(drive, filePath, 0, 5120);
		provider.downloadComplete(drive, filePath);
		assertEquals(5120, bytes.length);
		
		// clean up
		provider.delete(drive, filePath);
	}
	
	@Test
	@Tag("integration")
	public void testGetInputStream() throws IOException {
		SMBStorageProvider provider = new SMBStorageProvider();
		Drive drive = initializeTestDrive();
		String path = "sizedFiles/stateCodes_u1kb.csv";
		File file = FileUtil.getResourceFile(path);
		
		try {
			provider.upload(drive, path, file);
		}
		catch (IOException e) {
			// ignore - file might already exist
		}
		assertTrue(provider.exists(drive, path));

		int numBytes = 5;
		try (InputStream inputStream = provider.getInputStream(drive, path)) {
			byte[] bytes = new byte[numBytes];
			inputStream.read(bytes);
			assertEquals(numBytes, bytes.length);
			assertEquals("idsta", new String(bytes));
			
			inputStream.read(bytes);
			assertEquals(numBytes, bytes.length);
			assertEquals("te,la", new String(bytes));
			
			inputStream.read(bytes, 1, 2);
			assertEquals(numBytes, bytes.length);
			assertEquals("tbela", new String(bytes));
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
            File file = File.createTempFile(FileUtil.MDACA_PREFIX + "testSMB_", ".txt");
            file.deleteOnExit();

            Writer writer = new OutputStreamWriter(new FileOutputStream(file));
            writer.write("Sample file created on " + new Date() + "\n");
            writer.close();
            return file;
    }
}
