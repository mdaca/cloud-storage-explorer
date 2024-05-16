package com.spinsys.mdaca.storage.explorer.provider;

import static com.spinsys.mdaca.storage.explorer.io.FileUtil.getResourceFile;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;

import com.spinsys.mdaca.storage.explorer.io.BasicFile;
import com.spinsys.mdaca.storage.explorer.io.FileUtil;
import com.spinsys.mdaca.storage.explorer.io.PathProcessor;
import com.spinsys.mdaca.storage.explorer.model.DriveItem;
import com.spinsys.mdaca.storage.explorer.model.DriveQuery;
import com.spinsys.mdaca.storage.explorer.persistence.Drive;

public abstract class AbstractProviderIT {

    protected final String SAMPLE_FOLDER_PATH = "sampleFolder/";
    protected final String SAMPLE_FOLDER_PATH2 = "sampleFolder2/";
    protected final String INVALID_FOLDER_NAME = SAMPLE_FOLDER_PATH + "withTooMuchText/";

    protected final String FILE_ONE_PATH = SAMPLE_FOLDER_PATH + "fileOne.txt";
    protected final String FILE_TWO_PATH = SAMPLE_FOLDER_PATH + "fileTwo.txt";

    protected final String FILE_ONE_CONTENTS = "Some sample text";
    protected final String FILE_TWO_CONTENTS = "More sample text";

    protected abstract AbstractStorageProvider getProvider();

    protected abstract Drive buildDrive();

    protected AbstractStorageProvider provider = getProvider();
    protected Drive drive = buildDrive();
    
    /** The name of a directory that is expected to exist on a provider */
    protected String preExistingDir = "csv-dir";

    @Test
    @Order(10)
    public void testUploadAndExists() throws IOException {
        File fileOne = getResourceFile(FILE_ONE_PATH);
        File fileTwo = getResourceFile(FILE_TWO_PATH);

        if (provider.exists(drive, FILE_ONE_PATH)) {
        	provider.delete(drive, FILE_ONE_PATH);
        }

        if (provider.exists(drive, FILE_TWO_PATH)) {
        	provider.delete(drive, FILE_TWO_PATH);
        }

        provider.upload(drive, FILE_ONE_PATH, fileOne);
        provider.upload(drive, FILE_TWO_PATH, fileTwo);

        assertTrue(provider.exists(drive, FILE_ONE_PATH));
        assertTrue(provider.exists(drive, FILE_TWO_PATH));
    }

    @Test
    @Order(20)
    public void testIsDirectory() throws IOException {
        assertTrue(provider.isDirectory(drive, SAMPLE_FOLDER_PATH));
        assertTrue(provider.exists(drive, SAMPLE_FOLDER_PATH));

        assertFalse(provider.isDirectory(drive, INVALID_FOLDER_NAME));
        assertFalse(provider.exists(drive, INVALID_FOLDER_NAME));
    }

    @Test
    @Order(30)
    public void testFind() throws IOException {
        DriveQuery query = new DriveQuery(SAMPLE_FOLDER_PATH);

        List<DriveItem> driveItems = provider.find(drive, query);

        assertEquals(2, driveItems.size(),
                "Expected only 2 items inside folder, but instead found " + driveItems.size());
    }

    @Test
    @Order(40)
    public void testExists() throws IOException {
        assertTrue(provider.exists(drive, preExistingDir),
        			"expected a directory \"" + preExistingDir + "\" to exist");
        String dirWithFirstSlash = PathProcessor.addFirstSlash(preExistingDir);
		assertTrue(provider.exists(drive, dirWithFirstSlash),
    			"expected a directory \"" + dirWithFirstSlash + "\" to exist");
    }

    @Test
    @Order(100)
    public void testDownload() throws IOException {
        BasicFile fileOne = provider.download(drive, FILE_ONE_PATH);
        BasicFile fileTwo = provider.download(drive, FILE_TWO_PATH);

        assertEquals(fileOne.contents(), FILE_ONE_CONTENTS);
        assertEquals(fileTwo.contents(), FILE_TWO_CONTENTS);

        // clean up
        fileOne.delete();
        fileTwo.delete();
    }
    
    @Test
    @Order(110)
    public void testGetTopLines0() throws IOException {
        int numLines = 0;
		BasicFile fileOne = provider.getTopLines(drive, FILE_ONE_PATH, numLines);
        assertEquals("", fileOne.contents());

		List<String> fileOneContents = Files.readAllLines(Paths.get(fileOne.getCanonicalPath()));
		assertNotNull(fileOneContents);
		assertEquals(numLines, fileOneContents.size());

        // clean up
        fileOne.delete();
    }

    @Test
    @Order(120)
    public void testGetTopLines1() throws IOException {
        int numLines = 1;
		BasicFile fileOne = provider.getTopLines(drive, FILE_ONE_PATH, numLines);
        assertEquals(fileOne.contents(), FILE_ONE_CONTENTS);

		List<String> fileOneContents = Files.readAllLines(Paths.get(fileOne.getCanonicalPath()));
		assertNotNull(fileOneContents);
		assertEquals(numLines, fileOneContents.size());

        // clean up
        fileOne.delete();
    }

	@Test
    @Order(130)
	public void testGetInputStream() throws IOException {
		Drive drive = buildDrive();

		int numBytes = 5;
		try (InputStream inputStream = provider.getInputStream(drive, FILE_ONE_PATH)) {
			byte[] bytes = new byte[numBytes];
			inputStream.read(bytes);
			assertEquals(numBytes, bytes.length);
			assertEquals(FILE_ONE_CONTENTS.substring(0, 5), new String(bytes));
			
			inputStream.read(bytes);
			assertEquals(numBytes, bytes.length);
			assertEquals(FILE_ONE_CONTENTS.substring(5, 10), new String(bytes));
			
			inputStream.read(bytes, 1, 2);
			assertEquals(numBytes, bytes.length);
			assertEquals(FILE_ONE_CONTENTS.substring(5, 6) +
						 FILE_ONE_CONTENTS.substring(10, 12) +
						 FILE_ONE_CONTENTS.substring(8, 10),
						 new String(bytes));
		}
	}



    @Test
    @Order(1100)
    public void testDelete() throws IOException {

        if (!provider.exists(drive, FILE_ONE_PATH)) {
            File fileOne = getResourceFile(FILE_ONE_PATH);
            provider.upload(drive, FILE_ONE_PATH, fileOne);
        }

        if (!provider.exists(drive, FILE_TWO_PATH)) {
            File fileTwo = getResourceFile(FILE_TWO_PATH);
            provider.upload(drive, FILE_TWO_PATH, fileTwo);
        }
        
        assertTrue(provider.exists(drive, FILE_ONE_PATH));
        assertTrue(provider.exists(drive, FILE_TWO_PATH));

        provider.delete(drive, FILE_ONE_PATH);
        provider.delete(drive, FILE_TWO_PATH);

        assertFalse(provider.exists(drive, FILE_ONE_PATH));
        assertFalse(provider.exists(drive, FILE_TWO_PATH));
    }

    @Test
    @Order(2000)
    public void testMkdir() throws IOException {
        provider.mkdir(drive, SAMPLE_FOLDER_PATH);

        assertTrue(provider.isDirectory(drive, SAMPLE_FOLDER_PATH));

        //this has a placeholder file inside it, but it should be skipped in the search
        DriveQuery query = new DriveQuery(SAMPLE_FOLDER_PATH);
        int filesInsideSampleFolder = provider.find(drive, query).size();

        assertEquals(0, filesInsideSampleFolder);

        query.setUsesPlaceholder(true);
        filesInsideSampleFolder = provider.find(drive, query).size();

        assertEquals(1, filesInsideSampleFolder);

        provider.deleteDirectory(drive, SAMPLE_FOLDER_PATH);
        assertFalse(provider.exists(drive, SAMPLE_FOLDER_PATH));
    }

    @Test
    @Order(2900)
    public void testCopy() throws IOException {
        File localFileOne = FileUtil.getResourceFile(FILE_ONE_PATH);

        if (!provider.exists(drive, FILE_ONE_PATH)) {
            provider.upload(drive, FILE_ONE_PATH, localFileOne);
        }

        String newPath = "fileOneCopied.txt";
        provider.copy(drive, FILE_ONE_PATH, newPath);
        assertTrue(provider.exists(drive, newPath));
        assertTrue(provider.exists(drive, FILE_ONE_PATH));

        // clean up
        provider.delete(drive, newPath);
        provider.delete(drive, FILE_ONE_PATH);
        assertFalse(provider.exists(drive, newPath));
    }

    @Test
    @Order(3000)
    public void testCopyFirstSlash() throws IOException {
        File localFileOne = FileUtil.getResourceFile(FILE_ONE_PATH);

        if (!provider.exists(drive, FILE_ONE_PATH)) {
            provider.upload(drive, FILE_ONE_PATH, localFileOne);
        }

        String newPath = SAMPLE_FOLDER_PATH + "fileOneCopied.txt";
        provider.copy(drive, FILE_ONE_PATH, newPath);
        assertTrue(provider.exists(drive, newPath));
        assertTrue(provider.exists(drive, FILE_ONE_PATH));

        // clean up
        provider.delete(drive, newPath);
        provider.delete(drive, FILE_ONE_PATH);
        assertFalse(provider.exists(drive, newPath));
    }

    @Test
    @Order(3100)
    public void testRename() throws IOException {
        String newPath = FILE_TWO_PATH;
        
        // Avoid potential naming conflicts if newPath exists
        if (provider.exists(drive, newPath)) {
            provider.delete(drive, newPath);
        }        

        File localFileOne = FileUtil.getResourceFile(FILE_ONE_PATH);

        if (!provider.exists(drive, FILE_ONE_PATH)) {
            provider.upload(drive, FILE_ONE_PATH, localFileOne);
        }

        provider.rename(drive, FILE_ONE_PATH, newPath);
        assertTrue(provider.exists(drive, newPath));

        provider.delete(drive, newPath);
        assertFalse(provider.exists(drive, newPath));
    }

    @Test
    @Order(3300)
    public void testUploadRenameAndDeleteDirectory() throws IOException {
        File wordsFolder = new File("src/test/resources/words");

        if (provider.exists(drive, SAMPLE_FOLDER_PATH)) {
            provider.deleteDirectory(drive, SAMPLE_FOLDER_PATH);
        }

        provider.uploadDirectory(drive, SAMPLE_FOLDER_PATH, wordsFolder);
        assertTrue(provider.exists(drive, SAMPLE_FOLDER_PATH));
        assertTrue(provider.isDirectory(drive, SAMPLE_FOLDER_PATH));

        provider.rename(drive, SAMPLE_FOLDER_PATH, SAMPLE_FOLDER_PATH2);
        assertFalse(provider.exists(drive, SAMPLE_FOLDER_PATH));
        assertTrue(provider.exists(drive, SAMPLE_FOLDER_PATH2));
        assertTrue(provider.isDirectory(drive, SAMPLE_FOLDER_PATH2));

        provider.deleteDirectory(drive, SAMPLE_FOLDER_PATH2);
        assertFalse(provider.exists(drive, SAMPLE_FOLDER_PATH2));
    }


}
