package com.spinsys.mdaca.storage.explorer.rest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import javax.ws.rs.core.Response;

import org.junit.jupiter.api.Test;

class BaseServiceTest {

	@Test
	void testPopulateSuccessResponse() {
		BaseService service = new BaseService();
		Response response = service.populateSuccessResponse();
		assertNotNull(response);
		assertEquals(200, response.getStatus());
		
		response = service.populateSuccessResponse("entityValue");
		assertNotNull(response);
		assertEquals("entityValue", response.getEntity());
	}

}
