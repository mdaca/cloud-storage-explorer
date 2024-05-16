package com.spinsys.mdaca.storage.explorer.rest;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import com.spinsys.mdaca.storage.explorer.persistence.Drive;
import com.spinsys.mdaca.storage.explorer.persistence.DriveProperty;
import com.spinsys.mdaca.storage.explorer.persistence.DriveSecurityRule;

class AdminServiceTest extends AdminService {

	@Test
	public void testNullOutDrivesInProperties() {
		List<DriveProperty> properties = new ArrayList<>();

		List<Drive> drives = new ArrayList<>();

		for (int i = 0; i < 3; i++) {
			Drive drive = new Drive();
			drive.setDriveId(i);
			drives.add(drive);

			DriveProperty prop = new DriveProperty();
			prop.setDrive(drive );
			properties.add(prop);

			drive.setProviderProperties(Collections.singletonList(prop));
		}
		properties =
				properties.stream()
				.filter(p -> p.getDrive() != null)
				.collect(Collectors.toList());
		assertEquals(3, properties.size(), "All properties should have non-null drives.");

		drives.forEach(Drive::voidMappedClasses);

		properties =
				properties.stream()
				.filter(p -> p.getDrive() != null)
				.collect(Collectors.toList());
		assertEquals(0, properties.size(), "All properties should have null drives.");
	}

	@Test
	public void testNullOutDrivesInRules() {
		List<DriveSecurityRule> ruleList = new ArrayList<>();

		List<Drive> drives = new ArrayList<>();

		for (int i = 0; i < 3; i++) {
			Drive drive = new Drive();
			drive.setDriveId(i);
			drives.add(drive);

			DriveSecurityRule rule = new DriveSecurityRule();
			rule.setDrive(drive );
			ruleList.add(rule);

			drive.setSecurityRules(Collections.singletonList(rule));
		}
		ruleList =
				ruleList.stream()
				.filter(p -> p.getDrive() != null)
				.collect(Collectors.toList());
		assertEquals(3, ruleList.size(), "All rules should have non-null drives.");

		drives.forEach(Drive::voidMappedClasses);

		ruleList =
				ruleList.stream()
				.filter(p -> p.getDrive() != null)
				.collect(Collectors.toList());
		assertEquals(0, ruleList.size(), "All rules should have null drives.");
	}

}
