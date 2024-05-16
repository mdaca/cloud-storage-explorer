package com.spinsys.mdaca.storage.explorer.model;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;

class AWSS3RegionTest {

	@Test
	void testRegionRetrieval() {
		Region region = RegionUtils.getRegion("us-east-1");
		assertNotNull(region);
	}

}
