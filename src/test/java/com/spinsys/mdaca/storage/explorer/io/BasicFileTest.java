package com.spinsys.mdaca.storage.explorer.io;

import org.apache.commons.io.FileUtils;import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import static com.spinsys.mdaca.storage.explorer.io.FileUtil.getResourceFile;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BasicFileTest {

	/** Where to put the uncompressed artifacts during testing. */
    private static final String UNCOMPRESS_DIR =
    		(System.getenv("UncompressDir") == null)
    			? "C:/Temp/" : System.getenv("UncompressDir");
	private static final String ARCHIVE_FOLDER_PATH = "archiveFiles/";
    private static final String SAMPLE_FOLDER_PATH = "sampleFolder/";

    private static final String FILE_ONE_PATH = SAMPLE_FOLDER_PATH + "fileOne.txt";
    private static final String FILE_ONE_TAR_PATH = ARCHIVE_FOLDER_PATH + "fileOne.tar";
    private static final String JAR_PATH = ARCHIVE_FOLDER_PATH + "ab.jar";
    private static final String SAMPLES_TAR_PATH = ARCHIVE_FOLDER_PATH + "samples.tar";
    private static final String Z_PATH = ARCHIVE_FOLDER_PATH + "audits.html.Z";

    /**
     * contents of file /test/resources/text.txt
     */
    private final static String CONTENTS_OF_TEXT_TXT = "Lorem Ipsum";

    /**
     * contents of file inside zipped folder: /text/resources/
     */
    private final static String CONTENTS_OF_DOC1_DOCX = "E pluribus unum";
    private final static String CONTENTS_OF_FILE_txt = "Sapere aude";

    @Test
    public void testUnzipFile() throws IOException {
        //contents of /resources/compressedFile.zip
        // - /testFolder/doc1.docx
        // - /testFolder/file.txt
        String unzippedFileName = "src/test/resources/compressedFile";
        BasicFile zippedFile = new BasicFile(unzippedFileName + ".zip");

        BasicFile unzippedFolder = zippedFile.unZipFile(unzippedFileName);
        assertTrue(unzippedFolder.exists());

        BasicFile textFile = new BasicFile(unzippedFileName + "/testFolder/file.txt");
        assertEquals(textFile.contents(), CONTENTS_OF_FILE_txt);
        
        //TODO doc1.docx exists, but cannot be read by .contents()
//        BasicFile docFile = new BasicFile(unzippedFileName + "\\testFolder\\doc1.docx");
//        String docOneContents = docFile.contents();
//        Assertions.assertEquals(docOneContents, CONTENTS_OF_DOC1_DOCX);

        unzippedFolder.deleteDirectory();
        assertFalse(unzippedFolder.exists());
    }

    @Test
    public void testUngzipFile() throws IOException {
        String uncompressedFileName = "src/test/resources/text.txt";
        BasicFile compressedFile = new BasicFile(uncompressedFileName + ".gz");

        BasicFile uncompressedFile = compressedFile.unGzipFile(uncompressedFileName);

        assertTrue(uncompressedFile.exists());

        assertEquals(uncompressedFile.contents(), CONTENTS_OF_TEXT_TXT);

        uncompressedFile.delete();
        assertFalse(uncompressedFile.exists());
    }
    
    @Test
    public void testUnzipJarFile() throws IOException {
        File jarFile = getResourceFile(JAR_PATH);
        String jarBaseName = jarFile.getName().replace(".jar", "");
        String unjarredDir = UNCOMPRESS_DIR + jarBaseName;
        BasicFile compressedFile = new BasicFile(jarFile.getCanonicalPath());
		File newDir = compressedFile.untarFile(unjarredDir);
		File metaInfFile = new File(unjarredDir + "/META-INF");

        assertTrue(newDir.exists());
        assertTrue(metaInfFile.exists());

        // clean up
        FileUtils.deleteDirectory(newDir);
    }
    
    @Test
    public void testUntarOneFile() throws IOException {
        File fileOne = getResourceFile(FILE_ONE_PATH);
        File fileOneTar = getResourceFile(FILE_ONE_TAR_PATH);
        String tarBaseName = fileOneTar.getName().replace(".tar", "");
        String untarredDir = UNCOMPRESS_DIR + tarBaseName;
        String file1Name = fileOne.getName();
        BasicFile compressedFile = new BasicFile(fileOneTar.getCanonicalPath());
		File newDir = compressedFile.untarFile(untarredDir);
		File untarredFile1 = new File(untarredDir + "/" + file1Name);

        assertTrue(newDir.exists());
        assertTrue(untarredFile1.exists());
        assertEquals(fileOne.length(), untarredFile1.length());

        // clean up
        FileUtils.deleteDirectory(newDir);
    }
    
    @Test
    public void testUntarOneDir() throws IOException {
        File fileOne = getResourceFile(FILE_ONE_PATH);
        File samplesTar = getResourceFile(SAMPLES_TAR_PATH);
        String tarBaseName = samplesTar.getName().replace(".tar", "");
        String untarredDir = UNCOMPRESS_DIR + tarBaseName;
        String file1Name = fileOne.getName();
        BasicFile compressedFile = new BasicFile(samplesTar.getCanonicalPath());
		File newDir = compressedFile.untarFile(untarredDir);
		File untarredFile1 = new File(untarredDir + "/sampleFolder/" + file1Name);

        assertTrue(newDir.exists());
        assertTrue(untarredFile1.exists());
        assertEquals(fileOne.length(), untarredFile1.length());

        // clean up
        FileUtils.deleteDirectory(newDir);
    }
    
    @Test
    public void testWriteAndContents() throws IOException {
        BasicFile file = BasicFile.buildTempFile("testTemp.txt");

        String contents = "fair thee well";
        file.write(contents);
        
        assertEquals(file.contents(), contents);
        
        assertTrue(file.delete());
    }

    @Test
    public void testUncompressZArchive() throws IOException {
        File zFile = getResourceFile(Z_PATH);
        String zBaseName = zFile.getName().replace(".Z", "");
        String uncompressedFileS = PathProcessor.addLastSlash(UNCOMPRESS_DIR) + zBaseName;
        BasicFile compressedFile = new BasicFile(zFile.getCanonicalPath());
		File uncompressedFile =
				compressedFile.uncompressZArchive(uncompressedFileS);

        assertTrue(uncompressedFile.exists());
        String content = FileUtils.readFileToString(uncompressedFile, "UTF-8");
        assertTrue(content.contains("action_audit_id"));

        // clean up
        uncompressedFile.delete();
    }
    

}
