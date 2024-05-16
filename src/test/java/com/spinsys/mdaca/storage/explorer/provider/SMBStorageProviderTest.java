package com.spinsys.mdaca.storage.explorer.provider;

import static com.spinsys.mdaca.storage.explorer.provider.AWSS3StorageProvider.ACCESS_KEY_PROPERTY_KEY;
import static com.spinsys.mdaca.storage.explorer.provider.AWSS3StorageProvider.ACCESS_SECRET_PROPERTY_KEY;
import static com.spinsys.mdaca.storage.explorer.provider.AWSS3StorageProvider.BUCKET_NAME_PROPERTY_KEY;
import static org.junit.jupiter.api.Assertions.*;

import java.util.logging.Logger;

import com.spinsys.mdaca.storage.explorer.model.enumeration.DriveType;
import com.spinsys.mdaca.storage.explorer.persistence.Drive;

import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

/**
 * Contains unit tests for SMBStorageProvider
 * NOTE - for these tests to work certain env variables must be set:
 * @apiNote DriveService.*_PROPERTY_KEY
 */

@TestMethodOrder(OrderAnnotation.class)
public class SMBStorageProviderTest {
	
    private static final Logger logger =
            Logger.getLogger("com.spinsys.mdaca.storage.explorer.provider.SMBStorageProviderTest");

	/** Handy string to include in file names. */
//    private static String today = DayOfWeek.from(LocalDate.now()).toString();
            

	private SMBStorageProvider provider = new SMBStorageProvider();
	
	Drive drive = initializeTestDrive();

	    
	public static Drive initializeTestDrive() {
		Drive drive = new Drive();
		drive.setDisplayName("SMB1234");
		drive.setDriveType(DriveType.SMB);
		drive.setDriveId(2);
		drive.addPropertyValue(ACCESS_KEY_PROPERTY_KEY,
				System.getenv(ACCESS_KEY_PROPERTY_KEY));
		drive.addPropertyValue(ACCESS_SECRET_PROPERTY_KEY,
				System.getenv(ACCESS_SECRET_PROPERTY_KEY));
		drive.addPropertyValue(BUCKET_NAME_PROPERTY_KEY,
				System.getenv(BUCKET_NAME_PROPERTY_KEY));
		return drive;
	}


	@Test
    public void testIsDirectoryNull() {
		String pathIn = null;
		boolean pathOut = provider.isDirectory(null, pathIn);
    	assertFalse(pathOut);
    }
    
	@Test
    public void testNormalizePathNull() {
		String pathIn = null;
		String pathOut = provider.normalizePath(pathIn);
    	assertNull(pathOut);
    }
    
	@Test
    public void testNormalizePathNoOp() {
		String pathIn = "a\\b\\c";
		String pathOut = provider.normalizePath(pathIn);
    	assertEquals(pathIn, pathOut);
    }
    
	@Test
    public void testNormalizePathSimple() {
		String pathIn = "a/b/c";
		String pathOut = provider.normalizePath(pathIn);
    	assertEquals("a\\b\\c", pathOut);
    }

}
