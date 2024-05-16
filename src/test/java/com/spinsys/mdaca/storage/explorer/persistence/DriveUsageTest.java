package com.spinsys.mdaca.storage.explorer.persistence;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.spinsys.mdaca.storage.explorer.model.enumeration.DriveType;

import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

class DriveUsageTest {

	@Test
	void testGetSampling() {
		int numDesired = 5;
		
		int resultSize = 9;
		List<DriveMemoryUsageHistory> results = buildResultList(resultSize);
		
		List<DriveMemoryUsageHistory> sampling = DriveMemoryUsageHistory.getSampling(numDesired, results);
		assertEquals(numDesired, sampling.size(),
				"The sampling should have the requested number of elements");
		assertEquals(0, sampling.get(0).getUsageId(),
				"The sampling should include the first element of the result list");
		assertEquals(resultSize - 1, sampling.get(numDesired - 1).getUsageId(),
				"The sampling should include the last element of the result list");
		
		resultSize = 10;
		results = buildResultList(resultSize);
		
		sampling = DriveMemoryUsageHistory.getSampling(numDesired, results);
		assertEquals(0, sampling.get(0).getUsageId(),
				"The sampling should include the first element of the result list");
		assertEquals(numDesired, sampling.size(),
				"The sampling should have the requested number of elements");
		assertEquals(resultSize - 1, sampling.get(numDesired - 1).getUsageId(),
				"The sampling should include the last element of the result list");
		
		resultSize = 11;
		results = buildResultList(resultSize);
		
		sampling = DriveMemoryUsageHistory.getSampling(numDesired, results);
		assertEquals(0, sampling.get(0).getUsageId(),
				"The sampling should include the first element of the result list");
		assertEquals(numDesired, sampling.size(),
				"The sampling should have the requested number of elements");
		assertEquals(resultSize - 1, sampling.get(numDesired - 1).getUsageId(),
				"The sampling should include the last element of the result list");
	}

	/**
	 * Build a result list of the given size
	 */
	List<DriveMemoryUsageHistory> buildResultList(int resultSize) {
		List<DriveMemoryUsageHistory> results = new ArrayList<>(resultSize);
		
		for (int i = 0; i < resultSize ; i++) {
			DriveMemoryUsageHistory usage = new DriveMemoryUsageHistory();
			usage.setUsageId(i);
			results.add(usage);
		}
		return results;
	}
}
