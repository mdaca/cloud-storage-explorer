package com.spinsys.mdaca.storage.explorer.persistence;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.Test;

class ActionAuditTest {

	@Test
	void testSetStackTrace() {
		ActionAudit audit = new ActionAudit();
		String value = "errorString";
		audit.setStackTrace(value);
		assertEquals(value, audit.getStackTrace());
		
		value = RandomStringUtils.randomAlphanumeric(5000);
		assertEquals(5000, value.length());
		audit.setStackTrace(value);
		audit.setStackTrace(value);
		assertEquals(2048, audit.getStackTrace().length());		
	}

}
