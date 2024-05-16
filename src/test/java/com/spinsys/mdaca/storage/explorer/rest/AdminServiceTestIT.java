package com.spinsys.mdaca.storage.explorer.rest;

import com.spinsys.mdaca.storage.explorer.model.DriveListRequest;
import com.spinsys.mdaca.storage.explorer.model.enumeration.UserRole;
import com.spinsys.mdaca.storage.explorer.model.http.ActionAuditResponse;
import com.spinsys.mdaca.storage.explorer.persistence.ActionAudit;
import com.spinsys.mdaca.storage.explorer.persistence.Drive;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import java.util.Collections;

import static com.spinsys.mdaca.storage.explorer.persistence.ActionAudit.PENDING;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

class AdminServiceTestIT {

	@Test
	@Tag("integration")
	public void testCreateDriveWhenUnauthenticatedReturnsForbidden() {
		AdminService adminService = spy(AdminService.class);
		HttpServletRequest request = mock(HttpServletRequest.class);
		doReturn(UserRole.USER).when(adminService).getUserRole();
		DriveListRequest listRequest = new DriveListRequest();
		Drive drive = new Drive();
		listRequest.setDrives(Collections.singletonList(drive));
		doReturn(new ActionAudit()).when(adminService)
				.auditAction("createDrive", drive.toString(), 0, PENDING);
		doNothing().when(adminService).recordUnauthorized(any(ActionAudit.class));

		Response response = adminService.createDrives(request, listRequest);

		Assertions.assertNotNull(response);
		Assertions.assertTrue(response.getEntity() instanceof ActionAuditResponse);
	}

}
