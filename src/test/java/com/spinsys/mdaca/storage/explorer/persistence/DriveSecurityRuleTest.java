package com.spinsys.mdaca.storage.explorer.persistence;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

class DriveSecurityRuleTest {

	@Test
	void testSerialization() {
		Drive drive = new Drive();
		DriveSecurityRule rule = new DriveSecurityRule();
		rule.setDrive(drive);
		rule.setExclude(true);
		rule.setRoleName("Guest");
		rule.setRuleId(666);
		rule.setRuleText(".*");
		drive.setCreatedBy("testSerialization");
		drive.setDisplayName("testDrive");
		
		ObjectMapper mapper = new ObjectMapper();
		try {
			String ruleString = mapper.writeValueAsString(rule);
			assertTrue(ruleString.contains("" + rule.isExclude()),
					"The exclude info should be present.");
			assertTrue(ruleString.contains(rule.getRoleName()),
					"The role name should be present.");
			assertTrue(ruleString.contains("" + rule.getRuleId()),
					"The rule ID should be present.");
			assertTrue(ruleString.contains(rule.getRuleText()),
					"The rule text should be present.");
			assertFalse(ruleString.contains("rive"),
					"No drive information should be present.");
		} catch (JsonProcessingException e) {
			fail("testSerialization threw: " + e.getMessage());
		}
	}

	/**
	 * Tests the method DriveSecurityRule.passesRule(path)
	 * for relatively simple include cases.
	 */
	@Test
	public void testPassesRuleSimpleInclude() {
		DriveSecurityRule rule = new DriveSecurityRule();
		rule.setRuleText("folder/subfolder/.*");
		rule.setExclude(false);

		assertFalse(rule.passesRule("notRelevantFolder"));
		assertFalse(rule.passesRule("stillNotRelevantFolder"));
		assertFalse(rule.passesRule("folderSlashSubfolder"));

		assertTrue(rule.passesRule("folder/subfolder/file.txt"));
		assertTrue(rule.passesRule("folder/subfolder/anotherFolder/"));
	}

	/**
	 * Tests the method DriveSecurityRule.passesRule(path)
	 * for relatively simple exclude cases.
	 */
	@Test
	public void testPassesRuleSimpleExclude() {
		DriveSecurityRule rule = new DriveSecurityRule();
		rule.setRuleText("folder/subfolder/.*");
		rule.setExclude(true);

		assertTrue(rule.passesRule("notRelevantFolder"));
		assertTrue(rule.passesRule("stillNotRelevantFolder"));
		assertTrue(rule.passesRule("folderSlashSubfolder"));

		assertFalse(rule.passesRule("folder/subfolder/file.txt"));
		assertFalse(rule.passesRule("folder/subfolder/anotherFolder/"));
	}

	/**
	 * Tests the method DriveSecurityRule.passesRule(path)
	 * for include cases involving ancestor folders.
	 */
	@Test
	public void testFailsRuleAncestorInclude() {
		DriveSecurityRule rule = new DriveSecurityRule();
		rule.setRuleText("folder/subfolder/anotherSubFolder/.*");
		rule.setExclude(false);

		assertFalse(rule.passesRule("almostFolder/"));
		assertFalse(rule.passesRule("anotherSubFolder/"));
		assertFalse(rule.passesRule("folders/"));
		//NOTE one character difference ("s") should make the rule fail

		assertFalse(rule.passesRule("folder/"),
				"An initially matching folder should fail.");
		assertFalse(rule.passesRule("fol"),
				"Only an initially matching folder should pass,"
				+ " not just an initially matching string.");

		assertFalse(rule.passesRule("folder/subfolder/"),
				"An initially matching series of folders should fail.");
		assertFalse(rule.passesRule("subfolder/"),
				"Only an initially matching folder should pass,"
				+ " not an internally matching folder.");

		assertTrue(rule.passesRule("folder/subfolder/anotherSubFolder/"),
				"A match for the folder path should pass.");
		assertFalse(rule.passesRule("folder/subf"),
				"Only initially matching folders should pass,"
				+ " not just an initially matching string.");
	}

	/**
	 * Tests the method DriveSecurityRule.passesRule(path)
	 * for exclude cases involving ancestor folders.
	 */
	@Test
	public void testPassesRuleAncestorExclude() {
		DriveSecurityRule rule = new DriveSecurityRule();
		rule.setRuleText("folder/subfolder/anotherSubFolder/.*");
		rule.setExclude(true);

		assertTrue(rule.passesRule("almostFolder/"));
		assertTrue(rule.passesRule("anotherSubFolder/"));
		assertTrue(rule.passesRule("folders/"));

		assertTrue(rule.passesRule("folder/"),
				"An initially matching folder should pass.");
		assertTrue(rule.passesRule("fol"),
				"An initially matching string should pass,"
				+ " regardless of whether it's a folder.");

		assertTrue(rule.passesRule("folder/subfolder/"),
				"An initially matching series of folders should pass.");
		assertTrue(rule.passesRule("subfolder/"),
				"An internally matching folder should pass.");

		assertFalse(rule.passesRule("folder/subfolder/anotherSubFolder/"),
				"The folder matches and should be excluded.");
		assertFalse(rule.passesRule("folder/subfolder/anotherSubFolder/x.txt"),
				"A file matching the exclusion rule should fail.");
		
		// This rule is just like the previous one, except for the
		// last character.  This should allow the user to see
		// the folder, but not the folder contents.
		rule = new DriveSecurityRule();
		rule.setRuleText("folder/subfolder/anotherSubFolder/.+");
		rule.setExclude(true);

		assertTrue(rule.passesRule("folder/subfolder/anotherSubFolder/"),
				"The containing folder should not be excluded.");
		assertFalse(rule.passesRule("folder/subfolder/anotherSubFolder/x.txt"),
				"A file matching the exclusion rule should fail.");
		
	}


	/**
	 * tests the method DriveSecurityRule.ancestorDirectoryMatches(path)
	 */
	@Test
	public void testAncestorDirectoryMatches() {
		DriveSecurityRule rule = new DriveSecurityRule();
		rule.setRuleText("folder/subfolder/anotherSubFolder/.*");

		assertFalse(rule.matchesAncestorDirectory("almostFolder/"));
		assertFalse(rule.matchesAncestorDirectory("anotherSubFolder/"));
		assertFalse(rule.matchesAncestorDirectory("folders/"));
		//NOTE one character difference ("s") should make the rule fail

		assertTrue(rule.matchesAncestorDirectory("folder/"),
				"An initially matching folder should pass.");
		assertFalse(rule.matchesAncestorDirectory("fol"),
				"Only an initially matching folder should pass,"
				+ " not just an initially matching string.");

		assertTrue(rule.matchesAncestorDirectory("folder/subfolder/"),
				"An initially matching series of folders should pass.");
		assertFalse(rule.matchesAncestorDirectory("subfolder/"),
				"Only an initially matching folder should pass,"
				+ " not an internally matching folder.");

		assertTrue(rule.matchesAncestorDirectory("folder/subfolder/anotherSubFolder/"),
				"An initially matching series of folders should pass.");
		assertFalse(rule.matchesAncestorDirectory("folder/subf"),
				"Only initially matching folders should pass,"
				+ " not just an initially matching string.");
	}

}
