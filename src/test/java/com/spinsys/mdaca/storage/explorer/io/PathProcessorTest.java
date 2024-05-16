package com.spinsys.mdaca.storage.explorer.io;

import org.junit.jupiter.api.Test;

import static com.spinsys.mdaca.storage.explorer.io.PathProcessor.*;
import static com.spinsys.mdaca.storage.explorer.io.PathProcessor.getParentFolderPath;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PathProcessorTest {

	@Test
	public void testGetFileNameNull() {
		assertNull(getFileName(null));
	}
	
	@Test
	public void testGetFileNameNone() {
		assertEquals("", getFileName(""));
	}
	
	@Test
	public void testGetFileNameNoDir() {
		assertEquals("file.txt", getFileName("file.txt"));
	}
	
	@Test
	public void testGetDirFileName() {
		assertEquals("folder2", getFileName("folder1/folder2/"));
	}
	
	@Test
	public void testGetFileNameMultiDir() {
		assertEquals("file.txt", getFileName("/d1/d2/file.txt"));
	}
	
	@Test
	public void testGetExtensionNull() {
		assertNull(getExtension(null));
	}
	
	@Test
	public void testGetExtensionNone() {
		assertEquals("", getExtension("file"),
				"A file with no '.' should have an empty extension.");
	}
	
	@Test
	public void testGetExtensionNormal() {
		assertEquals("txt", getExtension("file.txt"),
				"'file.txt' should have an extension of 'txt'.");
	}
	
	@Test
	public void testGetExtensionMultidot() {
		assertEquals("txt", getExtension("some.file.txt"),
				"'some.file.txt' should have an extension of 'txt'.");
	}
	
	@Test
	public void testGetParentFolderNone() {
		assertEquals(GUI_SEP,
				getParentFolder(""),
				"The parent directory for an empty string is the root");
	}
	
	@Test
	public void testGetParentFolderNoDir() {
		assertEquals(GUI_SEP,
				getParentFolder("file.txt"),
				"Unless present in the path, the parent directory"
				+ " for a file is assumed to be the root");
	}
	
	@Test
	public void testGetParentFolderMultiDir() {
		assertEquals("/d1/d2", getParentFolder("/d1/d2/file.txt"));
	}
	
	@Test
	public void testGetParentFolderMultiDirWindows() {
		assertEquals("C:/d1/d2", getParentFolder("C:/d1/d2/file.txt"));
	}
	
	@Test
	public void testSameSourceAndDestination() {
		assertTrue(sameSourceAndDestination(1, "a", 1, "a"));
		assertTrue(sameSourceAndDestination(1, "a/", 1, "a"));
		assertTrue(sameSourceAndDestination(1, "a/", 1, "a/"));
		assertFalse(sameSourceAndDestination(1, "a", 1, "b"));
		assertFalse(sameSourceAndDestination(1, "a", 2, "a"));
		assertFalse(sameSourceAndDestination(1, "a/", 1, "a/b"));
	}

	@Test
	public void testGetParentFolderPath() {
		//test root-level folders
		assertEquals("/", getParentFolderPath("/"));
		assertEquals("/", getParentFolderPath("test/"));
		assertEquals("/", getParentFolderPath("/test"));
		assertEquals("/", getParentFolderPath("/test/"));

		//test subfolders
		assertEquals("test/", getParentFolderPath("test/subFolder"));
		assertEquals("test/", getParentFolderPath("/test/subFolder"));
		assertEquals("test/", getParentFolderPath("/test/subFolder/"));
	}

	@Test
	public void testAddFirstSlash() {
		assertEquals("/", addFirstSlash(""));
		assertEquals("/", addFirstSlash("/"));
		assertEquals("/test", addFirstSlash("test"));
		assertEquals("/test", addFirstSlash("/test"));
		assertEquals("/test/", addFirstSlash("test/"));
		assertEquals("/test/", addFirstSlash("/test/"));
	}

	@Test
	public void testAddLastSlash() {
		assertEquals("/", addLastSlash(""));
		assertEquals("/", addLastSlash("/"));
		assertEquals("test/", addLastSlash("test"));
		assertEquals("/test/", addLastSlash("/test"));
		assertEquals("test/", addLastSlash("test/"));
		assertEquals("/test/", addLastSlash("/test/"));
	}

	@Test
	public void testRemoveFirstSlash() {
		assertEquals("", removeFirstSlash(""));
		assertEquals("", removeFirstSlash("/"));
		assertEquals("test", removeFirstSlash("test"));
		assertEquals("test", removeFirstSlash("/test"));
		assertEquals("test/", removeFirstSlash("test/"));
		assertEquals("test/", removeFirstSlash("/test/"));
	}

	@Test
	public void testRemoveLastSlash() {
		assertEquals("", removeLastSlash(""));
		assertEquals("", removeLastSlash("/"));
		assertEquals("test", removeLastSlash("test"));
		assertEquals("/test", removeLastSlash("/test"));
		assertEquals("test", removeLastSlash("test/"));
		assertEquals("/test", removeLastSlash("/test/"));
	}
	
	@Test
	public void testConvertToUnixStylePath() {
		assertEquals("/folder/subFolder/", convertToUnixStylePath("\\folder\\subFolder\\"));
		assertEquals("/folder/subFolder/", convertToUnixStylePath("/folder/subFolder/"));
		assertEquals("/very/long/path/with/many/sub/folders", convertToUnixStylePath("\\very\\long\\path\\with\\many\\sub\\folders"));
	}

	@Test
	public void testConvertToWindowsStylePath() {
		assertEquals("\\folder\\subFolder\\", convertToWindowsStylePath("\\folder\\subFolder\\"));
		assertEquals("\\folder\\subFolder\\", convertToWindowsStylePath("/folder/subFolder/"));
		assertEquals("\\very\\long\\path\\with\\many\\sub\\folders", convertToWindowsStylePath("\\very\\long\\path\\with\\many\\sub\\folders"));
	}

}
