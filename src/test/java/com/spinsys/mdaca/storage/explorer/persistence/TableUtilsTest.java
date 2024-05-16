package com.spinsys.mdaca.storage.explorer.persistence;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class TableUtilsTest {

	@Test
	void testGetSafeValue() {
		String input = "errorString";
		String output = TableUtils.getSafeValue(input, 0);
		assertEquals(output, "");

		output = TableUtils.getSafeValue(input, 1);
		assertEquals(output, "e");

		output = TableUtils.getSafeValue(input, input.length());
		assertEquals(output, input);

		output = TableUtils.getSafeValue(input, 100);
		assertEquals(output, input);
	}

}
