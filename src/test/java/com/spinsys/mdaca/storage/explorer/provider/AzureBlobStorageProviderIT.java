package com.spinsys.mdaca.storage.explorer.provider;

import static com.spinsys.mdaca.storage.explorer.provider.AzureBlobStorageProvider.BLOB_CONNECTION_STRING_PROPERTY_KEY;
import static com.spinsys.mdaca.storage.explorer.provider.AzureBlobStorageProvider.BLOB_CONTAINER_NAME_PROPERTY_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Date;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import com.spinsys.mdaca.storage.explorer.io.BasicFile;
import com.spinsys.mdaca.storage.explorer.io.FileUtil;
import com.spinsys.mdaca.storage.explorer.model.DriveItem;
import com.spinsys.mdaca.storage.explorer.model.StorageClass;
import com.spinsys.mdaca.storage.explorer.model.enumeration.DriveType;
import com.spinsys.mdaca.storage.explorer.persistence.Drive;

import static com.spinsys.mdaca.storage.explorer.provider.AzureBlobStorageProvider.COOL;


@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class AzureBlobStorageProviderIT extends AbstractProviderIT {

	/** A file name for a local empty file. */
	private static final String EMPTY_FILE = "sizedFiles/emptyFile.txt";

	/** A file name for a pre-existing empty file. */
	private static final String EMPTY_FILE_AZURE = "keithSaysLeaveMeEmpty.txt";

	/** A file name for a file that should already exist. */
	private static final String PREEXISTING_FILE = "keithSaysLeaveMeAlone.txt";


    @Override
    protected AbstractStorageProvider getProvider() {
        return new AzureBlobStorageProvider();
    }

    /**
     * Creates a temporary file with text data
     * @return A new file with text data.
     * @throws IOException
     */
	File createSampleFile() throws IOException {
		File file = File.createTempFile(FileUtil.MDACA_PREFIX + "testAzure_", ".txt");
		file.deleteOnExit();

		Writer writer = new OutputStreamWriter(new FileOutputStream(file));
		writer.write("Sample file created on " + new Date() + "\n");
		writer.close();
		return file;
	}

    @Override
    protected Drive buildDrive() {
        Drive drive = new Drive();
        drive.setDisplayName("Azure Blob");
        drive.setDriveType(DriveType.Blob);
        drive.setDriveId(3);
        drive.addPropertyValue(BLOB_CONNECTION_STRING_PROPERTY_KEY,
                System.getenv(BLOB_CONNECTION_STRING_PROPERTY_KEY));
        drive.addPropertyValue(BLOB_CONTAINER_NAME_PROPERTY_KEY,
                System.getenv(BLOB_CONTAINER_NAME_PROPERTY_KEY));
        return drive;
    }

	@Test
	@Tag("integration")
	@Order(300)
	public void testUploadPart1() throws IOException {
		Drive drive = buildDrive();

		File sampleFile = createSampleFile();
		String fileName = sampleFile.getName();
		assertFalse(provider.exists(drive, fileName));

		String uploadId = provider.uploadPartStart(drive, fileName);
		byte[] bytes = Files.readAllBytes(Paths.get(sampleFile.getCanonicalPath()));
		provider.uploadPart(drive, fileName, bytes, 1);
		provider.uploadPartComplete(drive, fileName, uploadId);
		assertTrue(provider.exists(drive, fileName));

		// clean up
		provider.delete(drive, fileName);
		assertFalse(provider.exists(drive, fileName));
	}

	@Test
	@Tag("integration")
	@Order(301)
	public void testUploadPart2() throws IOException {
		Drive drive = buildDrive();

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
	}

	@Test
	@Tag("integration")
	@Order(302)
	public void testUploadPartEmptyFile() throws IOException {
		Drive drive = buildDrive();

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
	}

    @Test
    @Order(320)
    public void testUpdateStorageClass() throws IOException {
        if (!(provider instanceof CloudStorageProvider)) {
            return;
        }
        CloudStorageProvider<?> provider = (CloudStorageProvider<?>) this.provider;

        File localFileOne = FileUtil.getResourceFile(FILE_TWO_PATH);

        if (!this.provider.exists(drive, FILE_TWO_PATH)) {
            this.provider.upload(drive, FILE_TWO_PATH, localFileOne);
        }

        StorageClass coolStorageClass = new StorageClass(COOL, "Cool", false);

        provider.updateStorageClass(drive, FILE_TWO_PATH, coolStorageClass);
//        DriveQuery query = new DriveQuery(FILE_TWO_PATH);
//        List<DriveItem> driveItems = this.provider.find(drive, query);
        DriveItem driveItem = provider.getDriveItem(drive, FILE_TWO_PATH);
		assertEquals(coolStorageClass, driveItem.getStorageClass());

        this.provider.deleteDirectory(drive, SAMPLE_FOLDER_PATH);
        assertFalse(this.provider.isDirectory(drive, SAMPLE_FOLDER_PATH));
    }


	@Test
	@Tag("integration")
	@Order(370)
	public void testDownloadBytes() throws IOException {
		Drive drive = buildDrive();

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
	@Order(371)
	public void testDownloadBytesFromEmptyFile() throws IOException {
		Drive drive = buildDrive();
		int numBytes = 5;
		byte[] downloaded = provider.downloadBytes(drive, EMPTY_FILE_AZURE, 0, numBytes);
		assertEquals(0, downloaded.length);
	}
	
    @Test
    @Order(1200)
    public void voidTestDeleteWherePlaceholderFileIsMissing() throws IOException {
        //sampleFolder folder still exists from testDelete(), but with no contents

        File fileOne = new File("src/test/resources/" + FILE_ONE_PATH);

        try {
            provider.upload(drive, FILE_ONE_PATH, fileOne);
        }
        catch (FileAlreadyExistsException e) {
        	// ignore
        }
        
        //should return 1 because the placeholder file is skipped in evaluation
        List<DriveItem> contents = provider.findAllInPath(drive, SAMPLE_FOLDER_PATH);
		assertEquals(1, contents.size());

        //even though the placeholder file is deleted, deleting the last file should generate a new placeholder file
        // again, this keeps the folder from being deleted
        provider.delete(drive, FILE_ONE_PATH);

        //check that the directory still exists
        provider.isDirectory(drive, SAMPLE_FOLDER_PATH);

        //cleanup
        provider.delete(drive, SAMPLE_FOLDER_PATH);
        assertFalse(provider.isDirectory(drive, SAMPLE_FOLDER_PATH));
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


}
