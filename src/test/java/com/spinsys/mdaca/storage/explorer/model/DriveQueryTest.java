package com.spinsys.mdaca.storage.explorer.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DriveQueryTest {

    @Test
    public void testIsIncludedWithStartPath() {
        DriveItem driveItem = new DriveItem();
        driveItem.setPath("/hello/world/document.docx");
        DriveQuery driveQuery = new DriveQuery("hello/world/");

        assertTrue(driveQuery.isIncluded(driveItem));
    }

    @Test
    public void testIsIncludedRecursive() {
        DriveItem driveItem = new DriveItem();
        driveItem.setPath("/hello/world/document.docx");
        DriveQuery driveQuery = new DriveQuery("hello/");

        assertFalse(driveQuery.isIncluded(driveItem));

        driveQuery.setRecursive(true);
        assertTrue(driveQuery.isIncluded(driveItem));
    }

    @Test
    public void testIsIncludedWithFirstSlash() {
        DriveItem driveItem = new DriveItem();
        driveItem.setPath("/firstFolder/secondFolder/thirdFolder/bestDocument.docx");

        //note that "/firstFolder..." DOES have a preceding slash
        DriveQuery driveQuery = new DriveQuery("/firstFolder/secondFolder");

        //this should be false since
        assertFalse(driveQuery.isIncluded(driveItem));

        driveQuery.setRecursive(true);
        assertTrue(driveQuery.isIncluded(driveItem));
    }

    @Test
    public void testIsIncludedWithoutFirstSlash() {
        DriveItem driveItem = new DriveItem();
        driveItem.setPath("/firstFolder/secondFolder/thirdFolder/bestDocument.docx");

        //note that "firstFolder..." does NOT have a preceding slash
        DriveQuery driveQuery = new DriveQuery("firstFolder/secondFolder");

        //this should be false since
        assertFalse(driveQuery.isIncluded(driveItem));

        driveQuery.setRecursive(true);
        assertTrue(driveQuery.isIncluded(driveItem));
    }

    @Test
    public void testIsIncludedSameFolderAndSubfolder() {
        DriveItem driveItem = new DriveItem();
        driveItem.setPath("/folderWithSameName/folderWithSameName/bestDocument.docx");

        DriveQuery driveQuery = new DriveQuery("/folderWithSameName/folderWithSameName/");

        assertTrue(driveQuery.isIncluded(driveItem));

        driveQuery.setRecursive(true);
        assertTrue(driveQuery.isIncluded(driveItem));
    }

}
