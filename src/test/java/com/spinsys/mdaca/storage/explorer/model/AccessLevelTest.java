package com.spinsys.mdaca.storage.explorer.model;

import org.junit.jupiter.api.Test;

import static com.spinsys.mdaca.storage.explorer.model.enumeration.AccessLevel.Archive;
import static com.spinsys.mdaca.storage.explorer.model.enumeration.AccessLevel.Create;
import static com.spinsys.mdaca.storage.explorer.model.enumeration.AccessLevel.Delete;
import static com.spinsys.mdaca.storage.explorer.model.enumeration.AccessLevel.Modify;
import static com.spinsys.mdaca.storage.explorer.model.enumeration.AccessLevel.None;
import static com.spinsys.mdaca.storage.explorer.model.enumeration.AccessLevel.Read;
import static com.spinsys.mdaca.storage.explorer.model.enumeration.AccessLevel.Restore;
import static com.spinsys.mdaca.storage.explorer.model.enumeration.AccessLevel.isApplicableAccessLevel;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AccessLevelTest {

    @Test
    public void testValidAccessLevelNone() {
        assertFalse(isApplicableAccessLevel(None, Delete));
        assertFalse(isApplicableAccessLevel(None, Modify));
        assertFalse(isApplicableAccessLevel(None, Create));
        assertFalse(isApplicableAccessLevel(None, Read));
        assertFalse(isApplicableAccessLevel(None, Archive));
        assertFalse(isApplicableAccessLevel(None, Restore));
    }

    @Test
    public void testValidAccessLevelDelete() {
        assertTrue(isApplicableAccessLevel(Delete, Delete));
        assertTrue(isApplicableAccessLevel(Delete, Modify));
        assertTrue(isApplicableAccessLevel(Delete, Create));
        assertTrue(isApplicableAccessLevel(Delete, Read));
        assertFalse(isApplicableAccessLevel(Delete, Archive));
        assertFalse(isApplicableAccessLevel(Delete, Restore));
    }

    @Test
    public void testValidAccessLevelModify() {
        assertFalse(isApplicableAccessLevel(Modify, Delete));
        assertTrue(isApplicableAccessLevel(Modify, Modify));
        assertTrue(isApplicableAccessLevel(Modify, Create));
        assertTrue(isApplicableAccessLevel(Modify, Read));
        assertFalse(isApplicableAccessLevel(Modify, Archive));
        assertFalse(isApplicableAccessLevel(Modify, Restore));
    }

    @Test
    public void testValidAccessLevelCreate() {
        assertFalse(isApplicableAccessLevel(Create, Delete));
        assertFalse(isApplicableAccessLevel(Create, Modify));
        assertTrue(isApplicableAccessLevel(Create, Create));
        assertTrue(isApplicableAccessLevel(Create, Read));
        assertFalse(isApplicableAccessLevel(Create, Archive));
        assertFalse(isApplicableAccessLevel(Create, Restore));
    }

    @Test
    public void testValidAccessLevelRead() {
        assertFalse(isApplicableAccessLevel(Read, Delete));
        assertFalse(isApplicableAccessLevel(Read, Modify));
        assertFalse(isApplicableAccessLevel(Read, Create));
        assertTrue(isApplicableAccessLevel(Read, Read));
        assertFalse(isApplicableAccessLevel(Read, Archive));
        assertFalse(isApplicableAccessLevel(Read, Restore));
    }

    @Test
    public void testValidAccessLevelArchive() {
        assertFalse(isApplicableAccessLevel(Archive, Delete));
        assertFalse(isApplicableAccessLevel(Archive, Modify));
        assertFalse(isApplicableAccessLevel(Archive, Create));
        assertFalse(isApplicableAccessLevel(Archive, Read));
        assertTrue(isApplicableAccessLevel(Archive, Archive));
        assertFalse(isApplicableAccessLevel(Archive, Restore));
    }

    @Test
    public void testValidAccessLevelRestore() {
        assertFalse(isApplicableAccessLevel(Restore, Delete));
        assertFalse(isApplicableAccessLevel(Restore, Modify));
        assertFalse(isApplicableAccessLevel(Restore, Create));
        assertFalse(isApplicableAccessLevel(Restore, Read));
        assertFalse(isApplicableAccessLevel(Restore, Archive));
        assertTrue(isApplicableAccessLevel(Restore, Restore));
    }

}
