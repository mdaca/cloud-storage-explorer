package com.spinsys.mdaca.storage.explorer.persistence;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.spinsys.mdaca.storage.explorer.model.enumeration.DriveType;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

class DriveTest {

	@Test
	void testSerialization() {
		Drive drive = new Drive();
		DriveSecurityRule rule1 = new DriveSecurityRule();
		rule1.setDrive(drive);
		rule1.setExclude(true);
		drive.setCreated(new Date());
		drive.setCreatedBy("testSerialization");
		drive.setDisplayName("testDrive");
		drive.setDriveType(DriveType.S3);
		List<DriveSecurityRule> rules = new ArrayList<>();
		rules.add(rule1);
		drive.setSecurityRules(rules);
		drive.setUpdatedBy("me");
		
		ObjectMapper mapper = new ObjectMapper();
		try {
			String driveString = mapper.writeValueAsString(drive);
			assertTrue(driveString.contains(drive.getCreatedBy()),
					"The createdBy info  should be present.");
			assertTrue(driveString.contains(drive.getDisplayName()),
					"The drive name should be present.");
			assertTrue(driveString.contains(drive.getDriveType().toString()),
					"The drive type should be present.");
			assertTrue(driveString.contains(drive.getUpdatedBy()),
					"The updatedBy info should be present.");
			assertFalse(driveString.contains("Rule"),
					"No rule information should be present.");
		} catch (JsonProcessingException e) {
			fail("testSerialization threw: " + e.getMessage());
		}
	}
}
