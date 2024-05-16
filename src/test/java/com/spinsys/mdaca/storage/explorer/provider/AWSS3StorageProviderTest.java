package com.spinsys.mdaca.storage.explorer.provider;

import com.spinsys.mdaca.storage.explorer.model.DriveItem;
import com.spinsys.mdaca.storage.explorer.model.DriveQuery;
import com.spinsys.mdaca.storage.explorer.model.enumeration.DriveType;
import com.spinsys.mdaca.storage.explorer.persistence.Drive;

import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static com.spinsys.mdaca.storage.explorer.provider.AWSS3StorageProvider.ACCESS_KEY_PROPERTY_KEY;
import static com.spinsys.mdaca.storage.explorer.provider.AWSS3StorageProvider.ACCESS_SECRET_PROPERTY_KEY;
import static com.spinsys.mdaca.storage.explorer.provider.AWSS3StorageProvider.BUCKET_NAME_PROPERTY_KEY;
import static com.spinsys.mdaca.storage.explorer.provider.AWSS3StorageProvider.REGION_PROPERTY_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Contains unit tests for AWSS3StorageProvider
 * NOTE - for these tests to work certain env variables must be set:
 * @apiNote DriveService.*_PROPERTY_KEY
 */

@TestMethodOrder(OrderAnnotation.class)
public class AWSS3StorageProviderTest {
	
    private static final Logger logger =
            Logger.getLogger("com.spinsys.mdaca.storage.explorer.provider.AWSS3StorageProviderTest");

	/** Handy string to include in file names. */
//    private static String today = DayOfWeek.from(LocalDate.now()).toString();
            

	private AWSS3StorageProvider provider = new AWSS3StorageProvider();
	    
	public static Drive initializeTestDrive() {
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
    public void testIsDirectoryNull() {
        Drive drive = null;
        String path = null;
        assertTrue(!provider.isDirectory(drive, path));
    }

	@Test
    public void testNormalizePathNull() {
		String pathIn = null;
		String pathOut = provider.normalizePath(pathIn);
    	assertNull(pathOut);
    }
    
	@Test
    public void testNormalizePathNoOp() {
		String pathIn = "a/b/c";
		String pathOut = provider.normalizePath(pathIn);
    	assertEquals(pathIn, pathOut);
    }
    
	@Test
    public void testNormalizePathSimple() {
		String pathIn = "a\\b\\c";
		String pathOut = provider.normalizePath(pathIn);
    	assertEquals("a/b/c", pathOut);
    }
	
	@Test
	public void collectAllAtFirstLevel() throws IOException {
		Drive drive = initializeTestDrive();

		DriveQuery query = new DriveQuery();
		query.setDriveId(1);
		query.setSearchPattern("");
		query.setStartPath("Keith_Folder_W/Keith_SubFolder2/");

		List<DriveItem> driveItems = provider.find(drive, query);

		driveItems.forEach(System.out::println);
	}

	@Test
	public void collectAllUniqueDriveItemsInFirstLevel() {
		List<DriveItem> driveItems = buildDriveItemsList();

		DriveQuery driveQuery = new DriveQuery();
		List<DriveItem> uniqueDriveItemsList = driveItems.stream()
				.filter(driveQuery::isIncluded)
				.collect(Collectors.toList());

		//there should only be one entry for folder1, one entry for folder2, and 3 file entries (fileG.txt, fileH.txt, and fileI.txt)
		assertEquals(5, uniqueDriveItemsList.size());
	}

	@Test
	public void expectThreeUniqueDriveItemsInFolder1() {
		List<DriveItem> driveItems = buildDriveItemsList();

		DriveQuery query = new DriveQuery("/folder1");

		List<DriveItem> uniqueDriveItemsList = driveItems.stream()
				.filter(query::isIncluded)
				.collect(Collectors.toList());

		assertEquals(3, uniqueDriveItemsList.size());
	}

	@Test
	public void expectSubfolderWithSameNameIsFound() {
		List<DriveItem> driveItems = buildDriveItemsListDuplicateSubfolderName();

		DriveQuery query = new DriveQuery("/folderSameName");

		Set<DriveItem> filteredItems = driveItems.stream()
				.filter(query::isIncluded)
				.collect(Collectors.toSet());

		assertTrue(filteredItems.contains(new DriveItem(0, "/folderSameName/fileC.txt")));
		assertTrue(filteredItems.contains(new DriveItem(0, "/folderSameName/folderSameName/")));
	}

	@Test
	public void expectTwoUniqueDriveItemsInFolder2() {
		List<DriveItem> driveItems = buildDriveItemsList();

		DriveQuery query = new DriveQuery("/folder2");

		List<DriveItem> uniqueDriveItemsList = driveItems.stream()
				.filter(query::isIncluded)
				.collect(Collectors.toList());

		assertEquals(2, uniqueDriveItemsList.size());
	}

	private static List<DriveItem> buildDriveItemsList() {
		DriveItem di1 = new DriveItem();
		DriveItem di2 = new DriveItem();
		DriveItem di3 = new DriveItem();
		DriveItem di4 = new DriveItem();
		DriveItem di5 = new DriveItem();
		DriveItem di6 = new DriveItem();
		DriveItem di7 = new DriveItem();
		DriveItem di8 = new DriveItem();
		DriveItem di9 = new DriveItem();
		DriveItem di10 = new DriveItem();
		DriveItem di11 = new DriveItem();
		DriveItem di12 = new DriveItem();
		DriveItem di13 = new DriveItem();

		//inside folder 1
		di1.setPath("/folder1/subFolder1/fileA.png");
		di2.setPath("/folder1/subFolder1/fileB.docx");
		di3.setPath("/folder1/fileC.txt");
		di4.setPath("/folder1/fileD.java");

		//inside folder 2
		di5.setPath("/folder2/fileE.pdf");
		di6.setPath("/folder2/fileF.yaml");

		//files at the root level
		di7.setPath("fileG.tmnt");//intentionally removed first "/" for testing
		di9.setPath("/fileH.tla");
		di10.setPath("/fileI.exe");

		//set folder objects
		di11.setPath("/folder1/");
		di12.setPath("/folder2/");
		di13.setPath("/folder1/subFolder1/");

		return new ArrayList<>(Arrays.asList(di1, di2, di3, di4, di5, di6, di7, di8, di9, di10, di11, di12, di13));
	}

	private static List<DriveItem> buildDriveItemsListDuplicateSubfolderName() {
		DriveItem di1 = new DriveItem();
		DriveItem di2 = new DriveItem();
		DriveItem di3 = new DriveItem();
		DriveItem di4 = new DriveItem();
		DriveItem di5 = new DriveItem();

		di1.setPath("/folderSameName/folderSameName/fileA.png");
		di2.setPath("/folderSameName/folderSameName/fileB.docx");
		di3.setPath("/folderSameName/fileC.txt");
		di4.setPath("/folderSameName/folderSameName/");
		di5.setPath("/folderSameName/");

		return new ArrayList<>(Arrays.asList(di1, di2, di3, di4, di5));
	}

}
