package com.spinsys.mdaca.storage.explorer.provider;

import static com.spinsys.mdaca.storage.explorer.provider.BasicStorageProvider.DRIVE_NAME_PROPERTY_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.spinsys.mdaca.storage.explorer.model.enumeration.DriveType;
import com.spinsys.mdaca.storage.explorer.persistence.Drive;
import com.spinsys.mdaca.storage.explorer.persistence.DriveProperty;

import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

/**
 * Contains unit tests for WindowsStorageProvider
 */

@TestMethodOrder(OrderAnnotation.class)
public class WindowsStorageProviderTest {
	
	private WindowsStorageProvider provider = new WindowsStorageProvider();

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

	@Test
    public void testgetPathWithDriveLetterNullParameters() {
		assertThrows(NullPointerException.class, () -> provider.getPathWithDriveLetter(null, null));
    }
    
	@Test
    public void testgetPathWithDriveLetterRoot() {
		String pathIn = "/";
		Drive drive = new Drive();
		drive.setDriveType(DriveType.Windows);
		
		List<DriveProperty> props = new ArrayList<>();
		DriveProperty prop = new DriveProperty();
		prop.setPropertyKey(DRIVE_NAME_PROPERTY_KEY);
		prop.setPropertyValue("S");
		props.add(prop);
		drive.setProviderProperties(props);
		
		String pathOut = provider.getPathWithDriveLetter(drive, pathIn);
    	assertEquals("S:\\", pathOut);
    }
    
	@Test
    public void testgetPathWithDriveLetterMultiLevel() {
		String pathIn = "a/b/c";
		Drive drive = new Drive();
		drive.setDriveType(DriveType.Windows);
		
		List<DriveProperty> props = new ArrayList<>();
		DriveProperty prop = new DriveProperty();
		prop.setPropertyKey(DRIVE_NAME_PROPERTY_KEY);
		prop.setPropertyValue("S");
		props.add(prop);
		drive.setProviderProperties(props);
		
		String pathOut = provider.getPathWithDriveLetter(drive, pathIn);
    	assertEquals("S:\\a\\b\\c", pathOut);
    }
    
	@Test
    public void testgetPathWithDriveLetterMultiLevelSlashes() {
		String pathIn = "/a/b/c";
		Drive drive = new Drive();
		drive.setDriveType(DriveType.Windows);
		
		List<DriveProperty> props = new ArrayList<>();
		DriveProperty prop = new DriveProperty();
		prop.setPropertyKey(DRIVE_NAME_PROPERTY_KEY);
		prop.setPropertyValue("S");
		props.add(prop);
		drive.setProviderProperties(props);
		
		String pathOut = provider.getPathWithDriveLetter(drive, pathIn);
    	assertEquals("S:\\a\\b\\c", pathOut);
    }
    
	@Test
    public void testgetPathWithDriveLetterMultiLevelBackslashes() {
		String pathIn = "\\a\\b\\c";
		Drive drive = new Drive();
		drive.setDriveType(DriveType.Windows);
		
		List<DriveProperty> props = new ArrayList<>();
		DriveProperty prop = new DriveProperty();
		prop.setPropertyKey(DRIVE_NAME_PROPERTY_KEY);
		prop.setPropertyValue("S");
		props.add(prop);
		drive.setProviderProperties(props);
		
		String pathOut = provider.getPathWithDriveLetter(drive, pathIn);
    	assertEquals("S:\\a\\b\\c", pathOut);
    }
    
	@Test
    public void testgetPathWithDriveLetterMultiLevelBackslashesWithConsistentDrive() {
		String pathIn = "X:\\a\\b\\c";
		Drive drive = new Drive();
		drive.setDriveType(DriveType.Windows);
		
		List<DriveProperty> props = new ArrayList<>();
		DriveProperty prop = new DriveProperty();
		prop.setPropertyKey(DRIVE_NAME_PROPERTY_KEY);
		prop.setPropertyValue("X");
		props.add(prop);
		drive.setProviderProperties(props);
		
		String pathOut = provider.getPathWithDriveLetter(drive, pathIn);
    	assertEquals("X:\\a\\b\\c", pathOut);
    }

}
