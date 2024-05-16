package com.spinsys.mdaca.storage.explorer.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DriveItemTest {

    @Test
    public void testIsRoot() {
        DriveItem rootItem = new DriveItem("/");
        DriveItem nonRootItem = new DriveItem("folder/file.txt");

        assertTrue(rootItem.isRoot());
        assertFalse(nonRootItem.isRoot());
    }

    @Test
    public void testGetParentFolder() {
        DriveItem driveItem = new DriveItem("folder/file.txt");

        assertEquals("folder/", driveItem.getParentFolderPath());
    }

   @Test
    public void testGetParentItem() {
        DriveItem driveItem = new DriveItem("folder/file.txt");

       DriveItem parentItem = driveItem.getParentItem();
       assertEquals(new DriveItem("folder/"), parentItem);

       assertEquals(new DriveItem("/"), parentItem.getParentItem());
    }

    @Test
    public void testFileIsInsideFolderWithSlash() {
        DriveItem driveItem = new DriveItem();
        driveItem.setPath("/someFolder/anotherFolder/myFile.txt");

        assertTrue(driveItem.isInsideFolder("/someFolder"));
        assertTrue(driveItem.isInsideFolder("someFolder"));

        assertFalse(driveItem.isInsideFolder("/anotherFolder"));
        assertFalse(driveItem.isInsideFolder("anotherFolder"));

        assertFalse(driveItem.isInsideFolder("/unknownFolder"));
        assertFalse(driveItem.isInsideFolder("unknownFolder"));
    }

    @Test
    public void testFileIsInsideFolderWithoutSlash() {
        DriveItem driveItem = new DriveItem();
        driveItem.setPath("documents/files/loremIpsum.exe");

        assertTrue(driveItem.isInsideFolder("/documents"));
        assertTrue(driveItem.isInsideFolder("documents"));

        //should return false because "/files" is not the beginning of the path
        assertFalse(driveItem.isInsideFolder("/files"));
        assertFalse(driveItem.isInsideFolder("files"));
      
        assertFalse(driveItem.isInsideFolder("/notInPath"));
        assertFalse(driveItem.isInsideFolder("alsoNotInPath"));
    }

}
