package com.spinsys.mdaca.storage.explorer.io;

import static com.spinsys.mdaca.storage.explorer.io.FileUtil.getResourceFile;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.FileTime;
import java.util.Date;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;


public class FileUtilTest {

	public static final Logger logger =
		Logger.getLogger("com.spinsys.mdaca.storage.explorer.io.FileUtilTest");
	
	public static File createFileOfSize(String fileName, long fileSize) throws IOException {
		File file = new File(fileName);
		file.createNewFile();

		RandomAccessFile raf = new RandomAccessFile(file, "rw");
		// AWS S3 has a 5GB limit for some operations, so we make it slightly larger
		raf.setLength(fileSize);
		raf.close();
		return file;
	}

    @Test
    public void testGetTempFile() throws IOException {
        BasicFile file = FileUtil.buildTempFile("fileUtilTest");
        String contents = "fair thee well";
        file.write(contents);
        assertEquals(file.contents(), contents);
        
        // clean up
        file.delete();
    }

    /**
     * Tries to test that autodeletion works.  This is made tricky, because
     * we can't guarantee when garbage collection occurs, and our
     * autodeletion depends on GC.
     */
    @Disabled("Sporadic results dependeding on when GC occurs")
    @Test
    public void testGetTempFileAutoDeletion() throws IOException, InterruptedException {
    	int numFilesToCreate = 10_000;
        int timeToSleepMs = 10;

		for (int i = 0; i < numFilesToCreate; i++) {
            BasicFile file = FileUtil.buildTempFile("fileUtilTest" + i);
            String contents = "fileUtilTest#" + i;
            file.write(contents);
            assertEquals(file.contents(), contents);
            file = null;
			Thread.sleep(timeToSleepMs);
    	}
		
		String tmpdir = System.getProperty("java.io.tmpdir");
		File dir = new File(tmpdir);
		File[] files = dir.listFiles((d, name) -> name.startsWith("mdaca_fileUtilTest"));
		logger.info("deleted " + (numFilesToCreate - files.length) +
					" fileUtilTest* files");
		assertTrue(files.length < numFilesToCreate);
    }

    /**
     * Tries to test that autodeletion works.  This is made tricky, because
     * we can't guarantee when garbage collection occurs, and our
     * autodeletion depends on GC.  This test mixes in some manual deletes,
     * to make sure that doesn't cause problems.
     */
    @Disabled("Sporadic results dependeding on when GC occurs")
    @Test
    public void testGetTempFileMixedDeletion() throws IOException, InterruptedException {
    	int numFilesToCreate = 1_000;

		for (int i = 0; i < numFilesToCreate; i++) {
            BasicFile file = FileUtil.buildTempFile("fileUtilTest" + i);
            String contents = "fileUtilTest#" + i;
            file.write(contents);
            assertEquals(file.contents(), contents);
            
            int randomNum = ThreadLocalRandom.current().nextInt(1, 20);
            if (i % randomNum == 0) {
            	logger.info("Manually deleting " + file.getName());
            	file.delete();
            }
            file = null;
            int timeToSleepMs = ThreadLocalRandom.current().nextInt(1, 100);
			Thread.sleep(timeToSleepMs);
    	}
		
		String tmpdir = System.getProperty("java.io.tmpdir");
		File dir = new File(tmpdir);
		File[] files = dir.listFiles((d, name) -> name.startsWith("mdaca_fileUtilTest"));
		logger.info("deleted " + (numFilesToCreate - files.length) +
					" fileUtilTest* files");
		assertTrue(files.length < numFilesToCreate);
    }
    
    @Test
    public void testDeleteTempFilesAfterMinutes() throws IOException {
        BasicFile file10 = FileUtil.buildTempFile("fileUtilTest10");
        BasicFile file30 = FileUtil.buildTempFile("fileUtilTest30");
        BasicFile fileNow = FileUtil.buildTempFile("fileUtilTestNow");
    	assertTrue(fileNow.exists());
    	assertTrue(file10.exists());
    	assertTrue(file30.exists());

        resetFileTimeMinutesAgo(file10, 10); // 10 minutes ago
        resetFileTimeMinutesAgo(file30, 30); // 30 minutes ago

    	FileUtil.deleteTempFilesAfterMinutes(5);
    	assertTrue(fileNow.exists());
    	assertFalse(file10.exists());
    	assertFalse(file30.exists());
    }

    @Test
    public void testSpaceExists0() throws IOException {
    	assertTrue(FileUtil.spaceExists(0));
    }

    @Test
    public void testSpaceExists9tb() throws IOException {
        BasicFile file1hr = FileUtil.buildTempFile("testSpaceExists9tb1hr");
        BasicFile file2hr = FileUtil.buildTempFile("testSpaceExists9tb2hr");
        BasicFile fileNow = FileUtil.buildTempFile("testSpaceExists9tbNow");
    	assertTrue(fileNow.exists());
    	assertTrue(file1hr.exists());
    	assertTrue(file2hr.exists());

        resetFileTimeMinutesAgo(file1hr, 60); // 1 hour ago
        resetFileTimeMinutesAgo(file2hr, 120); // 2 hours ago

        // This test assumes there aren't 9 terabytes available
        assertFalse(FileUtil.spaceExists(9_000_000_000_000L));
        
        // Because we couldn't have created enough space,
        // no files got deleted
    	assertTrue(fileNow.exists());
    	assertTrue(file1hr.exists());
    	assertTrue(file2hr.exists());
    }
    
    @Test
    public void testGetTimestamp() {
    	String t1 = FileUtil.getTimestamp();
    	String t2 = FileUtil.getTimestamp();
    	
    	try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			// ignore
		}
    	String t3 = FileUtil.getTimestamp();
    	assertTrue(t1.compareTo(t2) <= 0);
    	assertTrue(t1.compareTo(t3) < 0);
    }

    @Disabled("Mocking/spying isn't set up correctly")
    @Test
    public void testSpaceExistsClearSome() throws IOException {
        BasicFile file1hr = FileUtil.buildTempFile("testSpaceExistsClearSome1hr");
        BasicFile file12hr = FileUtil.buildTempFile("testSpaceExistsClearSome12hr");
        BasicFile fileNow = FileUtil.buildTempFile("testSpaceExistsClearSomeNow");
        File twoMbFile = getResourceFile("sizedFiles/MDACA_CloudStorage_UserGuide_1.0_o2mb.docx");
        FileUtils.copyFile(twoMbFile, file1hr);
        FileUtils.copyFile(twoMbFile, file12hr);
        FileUtils.copyFile(twoMbFile, fileNow);
		
		// Make it so that there are only 2 MB available at first,
		// then 2 MB more each time a deletion (should have) occurred
		File tmpSpy = Mockito.spy(FileUtil.TMP_DIR);
//		when(tmpSpy.getUsableSpace()).thenReturn(2_000_000L, 4_000_000L, 6_000_000L);
		Mockito.doReturn(2_000_000L, 4_000_000L, 6_000_000L).when(tmpSpy).getUsableSpace();

//		Files.write(Paths.get(file1hr.getCanonicalPath()), content );
    	assertTrue(fileNow.exists());
    	assertTrue(file1hr.exists());
    	assertTrue(file12hr.exists());

        resetFileTimeMinutesAgo(fileNow, 0); // now
        resetFileTimeMinutesAgo(file1hr, 60); // 1 hour ago
        resetFileTimeMinutesAgo(file12hr, 720); // 12 hours ago

        // We ask for 3 MB, but there are only 2 MB available,
        // so we should see file12hr get deleted, but no others
        assertTrue(FileUtil.spaceExists(3_000_000L));
        
        assertTrue(fileNow.exists());
    	assertTrue(file1hr.exists());
    	assertFalse(file12hr.exists());
    }

	protected void resetFileTimeMinutesAgo(BasicFile file, int numMinutes) throws IOException {
		Date now = new Date();
        BasicFileAttributeView attributes =
        		Files.getFileAttributeView(Paths.get(file.getCanonicalPath()), BasicFileAttributeView.class);
        long nowMs = now.getTime();
        long thenMs = nowMs - (1000 * 60 * numMinutes);
		FileTime newTime = FileTime.fromMillis(thenMs);
        attributes.setTimes(newTime, newTime, newTime);
	}    

}
