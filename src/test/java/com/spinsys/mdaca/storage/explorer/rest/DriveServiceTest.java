package com.spinsys.mdaca.storage.explorer.rest;

import com.spinsys.mdaca.storage.explorer.model.enumeration.AccessLevel;
import com.spinsys.mdaca.storage.explorer.model.enumeration.DriveType;
import com.spinsys.mdaca.storage.explorer.model.http.ActionAuditResponse;
import com.spinsys.mdaca.storage.explorer.model.http.TransferSpec;
import com.spinsys.mdaca.storage.explorer.persistence.ActionAudit;
import com.spinsys.mdaca.storage.explorer.persistence.Drive;
import com.spinsys.mdaca.storage.explorer.persistence.DriveSecurityRule;

import org.junit.BeforeClass;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import javax.persistence.EntityManager;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.spinsys.mdaca.storage.explorer.persistence.ActionAudit.ERROR;
import static com.spinsys.mdaca.storage.explorer.provider.AWSS3StorageProvider.ACCESS_KEY_PROPERTY_KEY;
import static com.spinsys.mdaca.storage.explorer.provider.AWSS3StorageProvider.ACCESS_SECRET_PROPERTY_KEY;
import static com.spinsys.mdaca.storage.explorer.provider.AWSS3StorageProvider.BUCKET_NAME_PROPERTY_KEY;
import static com.spinsys.mdaca.storage.explorer.provider.AWSS3StorageProvider.REGION_PROPERTY_KEY;
import static com.spinsys.mdaca.storage.explorer.provider.BasicStorageProvider.DRIVE_NAME_PROPERTY_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

class DriveServiceTest {

	private static HttpServletRequest requestInRoleTrue = Mockito.mock(HttpServletRequest.class);
	private static HttpServletRequest requestInRoleFalse = Mockito.mock(HttpServletRequest.class);
	private static DriveService driveService = new DriveService();

	private static Drive drive;

	/**
	 * A file name for a Windows file that should already exist.
	 */
	private static final String PREEXISTING_WINDOWS_FILE = "Temp/t1.txt";

	@BeforeClass
	public static void setupClass() {
	}

	@BeforeAll
	public static void setupMethods() {
		doReturn(true).when(requestInRoleTrue).isUserInRole(anyString());
		doReturn(false).when(requestInRoleFalse).isUserInRole(anyString());
		drive = new Drive();
	}

	@Test
	public void testAssertDriveItemAccessInclude() {

		String path = "a";

		drive.setSecurityRules(Collections.singletonList(initializeSecurityRule("admin", "a", true)));
		assertFalse(driveService.assertDriveItemAccess(path, drive, AccessLevel.Read));
	}

	@Test
	public void testAssertDriveItemAccessExclude() {

		String path = "a";

		drive.setSecurityRules(Collections.singletonList(initializeSecurityRule("admin", "a", true)));
		assertFalse(driveService.assertDriveItemAccess(path, drive, AccessLevel.Read));

		drive.setSecurityRules(Collections.singletonList(initializeSecurityRule("admin", "b", true)));
		assertFalse(driveService.assertDriveItemAccess(path, drive, AccessLevel.Read),
				"Having a single rule excluding access to 'b' should not provide access to 'a'");
	}

	@Test
	public void testAssertDriveItemAccessAll() {
		String path = "a";

		drive.setSecurityRules(Collections.singletonList(initializeSecurityRule("admin", ".*", false)));
		assertTrue(driveService.assertDriveItemAccess(path, drive, AccessLevel.Read));

		// Having a single exclude rule does not provide access
		drive.setSecurityRules(Collections.singletonList(initializeSecurityRule("admin", ".*", true)));
		assertFalse(driveService.assertDriveItemAccess(path, drive, AccessLevel.Read),
				"Having a single exclude rule should not provide access");
	}

	@Test
	public void testAssertDriveItemAccessMismatch() {
		String path = "a";

		drive.setSecurityRules(Collections.singletonList(initializeSecurityRule("admin", "b", false)));
		assertFalse(driveService.assertDriveItemAccess(path, drive, AccessLevel.Read));
	}

	@Test
	public void testAssertDriveItemAccessBothMatchOr() {
		String path = "a";

		drive.setSecurityRules(Collections.singletonList(initializeSecurityRule("admin", "a|b", false)));
		assertTrue(driveService.assertDriveItemAccess(path, drive, AccessLevel.Read));

		path = "b";
		drive.setSecurityRules(Collections.singletonList(initializeSecurityRule("admin", "a|b", false)));
		assertTrue(driveService.assertDriveItemAccess(path, drive, AccessLevel.Read));
	}

	@Test
	public void testAssertDriveItemAccessMismatchRole() {
		DriveSecurityRule rule = initializeSecurityRule("admin", "b", false);
		doReturn(false).when(rule).isApplicableToEvaluate(any(AccessLevel.class));
		String path = "a";

		drive.setSecurityRules(Collections.singletonList(rule));
		//role doesn't apply to this user and path; because a rule must INCLUDE item to have assertDriveItemAccess() return true
		//because of this, return false
		assertFalse(driveService.assertDriveItemAccess(path, drive, AccessLevel.Read));
	}

	private DriveSecurityRule initializeSecurityRule(String roleName, String regex, boolean doExclude) {
		DriveSecurityRule rule = spy(DriveSecurityRule.class);

		rule.setRoleName(roleName);
		rule.setRuleText(regex);
		rule.setExclude(doExclude);
		rule.setAccessLevel("D");

		doReturn(true).when(rule).isApplicableToEvaluate(any(AccessLevel.class));
		return rule;
	}

	public Drive initializeWindowsTestDrive() {
		Drive drive = new Drive();
		drive.setDisplayName("DriveServiceTest Windows Test Drive");
		drive.setDriveType(DriveType.Windows);
		drive.setDriveId(2);
		drive.addPropertyValue(DRIVE_NAME_PROPERTY_KEY, "C");
		DriveSecurityRule rule = initializeSecurityRule("", ".*", false);
		List<DriveSecurityRule> rules = new ArrayList<>();
		rules.add(rule);
		drive.setSecurityRules(rules);
		return drive;
	}

	public static Drive initializeS3TestDrive() {
		Drive drive = new Drive();
		drive.setDisplayName("DriveServiceTest S3 Test Drive");
		drive.setDriveType(DriveType.S3);
		drive.setDriveId(1);
		drive.addPropertyValue(ACCESS_KEY_PROPERTY_KEY,
				System.getenv(ACCESS_KEY_PROPERTY_KEY));
		drive.addPropertyValue(ACCESS_SECRET_PROPERTY_KEY,
				System.getenv(ACCESS_SECRET_PROPERTY_KEY));
		drive.addPropertyValue(BUCKET_NAME_PROPERTY_KEY,
				System.getenv(BUCKET_NAME_PROPERTY_KEY));
		drive.addPropertyValue(REGION_PROPERTY_KEY,
	               "us-east-1");
		return drive;
	}

	@Test
	public void testTransferNoSourceDrive() throws IOException {
		HttpServletRequest mockedRequest = Mockito.mock(HttpServletRequest.class);
		StringBuffer url = new StringBuffer("http://someurl.com");
		when(mockedRequest.getRequestURL()).thenReturn(url);
		when(mockedRequest.getUserPrincipal()).thenReturn(() -> "userName");
		DriveService serviceSpy = Mockito.spy(new DriveService());
		TransferSpec spec = buildTransferSpec();
		spec.setSourceDriveId(1);

		doReturn(null).when(serviceSpy).getDrive(anyInt());
		mockAudit(spec, serviceSpy, "copy");
		doReturn(null).when(serviceSpy).recordException(any(ActionAudit.class), any(Exception.class), any(Long.class));

		javax.ws.rs.core.Response result =
				serviceSpy.transfer(spec, mockedRequest, null);
		assertNotNull(result);
		assertEquals(BaseService.INTERNAL_SERVER_ERROR, result.getStatus());
	}

	TransferSpec buildTransferSpec() {
		TransferSpec spec = new TransferSpec();
		spec.setDestDriveId(1);
		spec.setDestPath("a");
		spec.setRemoveSource(false);
		spec.setSourceDriveId(1);
		spec.setSourcePath("b");
		return spec;
	}

	TransferSpec buildLocalWindowsFileTransferSpec() {
		TransferSpec spec = new TransferSpec();
		spec.setSourceDriveId(2);
		spec.setSourcePath(PREEXISTING_WINDOWS_FILE);
		spec.setDestDriveId(2);
		spec.setDestPath(PREEXISTING_WINDOWS_FILE + "_2");
		spec.setRemoveSource(false);
		return spec;
	}

	@Test
	public void testRenameMissingSourceProvider() throws IOException {
		HttpServletRequest mockedRequest = Mockito.mock(HttpServletRequest.class);
		StringBuffer url = new StringBuffer("http://someurl.com");
		when(mockedRequest.getRequestURL()).thenReturn(url);
		when(mockedRequest.getUserPrincipal()).thenReturn(() -> "userName");

		DriveService service = Mockito.spy(new DriveService());
		Drive sourceDrive = initializeWindowsTestDrive();

		TransferSpec spec = buildLocalWindowsFileTransferSpec();
		spec.setSourceDriveId(sourceDrive.getDriveId());

		EntityManager manager = Mockito.mock(EntityManager.class);
		when(manager.createQuery(anyString())).thenReturn(null);
		doReturn(null).when(service).getDrive(eq(2));
		doReturn("").when(service).recordException(any(ActionAudit.class), any(Exception.class));
		mockAudit(spec, service, "rename");

		Response result = service.rename(spec, mockedRequest, null);
		assertNotNull(result);
		assertEquals(BaseService.INTERNAL_SERVER_ERROR, result.getStatus());
		assertEquals(500, result.getStatus());
	}

	@Test
	public void testRenameMissingDestProvider() throws IOException {
		HttpServletRequest mockedRequest = Mockito.mock(HttpServletRequest.class);
		StringBuffer url = new StringBuffer("http://someurl.com");
		when(mockedRequest.getRequestURL()).thenReturn(url);
		when(mockedRequest.getUserPrincipal()).thenReturn(() -> "userName");

		Drive sourceDrive = initializeWindowsTestDrive();
		int sourceDriveId = sourceDrive.getDriveId();

		TransferSpec spec = buildLocalWindowsFileTransferSpec();
		spec.setSourceDriveId(sourceDriveId);
		spec.setDestDriveId(666);
		spec.setRemoveSource(true);

		DriveService serviceSpy = Mockito.spy(DriveService.class);
		doReturn(sourceDrive).when(serviceSpy).getDrive(sourceDriveId);
		doReturn(null).when(serviceSpy).getDrive(666);
		doReturn(null).when(serviceSpy).recordException(any(ActionAudit.class), any(Exception.class), any(Long.class));
		mockAudit(spec, serviceSpy, "move");

		Response result = serviceSpy.transfer(spec, mockedRequest, null);
		assertNotNull(result);
		assertEquals(BaseService.INTERNAL_SERVER_ERROR, result.getStatus());
	}

	@Test
	public void testRenameMissingDestPath() throws IOException {
		HttpServletRequest mockedRequest = Mockito.mock(HttpServletRequest.class);
		StringBuffer url = new StringBuffer("http://someurl.com");
		when(mockedRequest.getRequestURL()).thenReturn(url);
		when(mockedRequest.getUserPrincipal()).thenReturn(() -> "userName");

		Drive sourceDrive = initializeWindowsTestDrive();
		int sourceDriveId = sourceDrive.getDriveId();

		TransferSpec spec = buildLocalWindowsFileTransferSpec();
		spec.setSourceDriveId(sourceDriveId);
		spec.setDestDriveId(sourceDriveId);
		spec.setDestPath(null);

		DriveService service = new DriveService();
		DriveService serviceSpy = Mockito.spy(service);
		doReturn(sourceDrive).when(serviceSpy).getDrive(sourceDriveId);
		mockAudit(spec, serviceSpy, "rename");
		doReturn("").when(serviceSpy).recordException(any(ActionAudit.class), any(Exception.class));

		Response result =
				serviceSpy.rename(spec, mockedRequest, null);
		assertNotNull(result);
		assertEquals(BaseService.INTERNAL_SERVER_ERROR, result.getStatus());
		assertEquals(500, result.getStatus());
	}

	@Test
	public void testRenameMissingSourcePath() throws IOException {
		HttpServletRequest mockedRequest = Mockito.mock(HttpServletRequest.class);
		StringBuffer url = new StringBuffer("http://someurl.com");
		when(mockedRequest.getRequestURL()).thenReturn(url);
		when(mockedRequest.getUserPrincipal()).thenReturn(() -> "userName");

		Drive sourceDrive = initializeWindowsTestDrive();
		int sourceDriveId = sourceDrive.getDriveId();

		TransferSpec spec = buildLocalWindowsFileTransferSpec();
		spec.setSourceDriveId(sourceDriveId);
		spec.setDestDriveId(sourceDriveId);
		spec.setDestPath("a");
		spec.setSourcePath(null);
		spec.setRemoveSource(true);

		DriveService service = new DriveService();
		DriveService serviceSpy = Mockito.spy(service);
		doReturn(sourceDrive).when(serviceSpy).getDrive(sourceDriveId);
		doReturn("").when(serviceSpy).recordException(any(ActionAudit.class), any(Exception.class));
		mockAudit(spec, serviceSpy, "rename");

		Response result =
				serviceSpy.rename(spec, mockedRequest, null);
		assertNotNull(result);
		assertEquals(BaseService.INTERNAL_SERVER_ERROR, result.getStatus());
		assertEquals(500, result.getStatus());
	}

	private void mockAudit(TransferSpec spec, DriveService serviceSpy, String action) {
		doReturn(new ActionAudit()).when(serviceSpy)
				.auditAction(action, spec.getSourcePath(), spec.getSourceDriveId(), "P",
                        spec.getDestPath(), spec.getDestDriveId(), null, null);
	}

	private void mockAudit(TransferSpec spec, DriveService serviceSpy, String action, String status, String message, String stackTrace) {
		doReturn(new ActionAudit()).when(serviceSpy)
				.auditAction(action, spec.getSourcePath(), spec.getSourceDriveId(), status,
                        spec.getDestPath(), spec.getDestDriveId(), message, stackTrace);
	}

	@Test
	public void testTransferMissingSourceProvider() throws IOException {
		HttpServletRequest mockedRequest = Mockito.mock(HttpServletRequest.class);
		StringBuffer url = new StringBuffer("http://someurl.com");
		when(mockedRequest.getRequestURL()).thenReturn(url);
		when(mockedRequest.getUserPrincipal()).thenReturn(() -> "userName");

		DriveService service = Mockito.spy(DriveService.class);
		Drive sourceDrive = initializeWindowsTestDrive();

		TransferSpec spec = buildLocalWindowsFileTransferSpec();
		spec.setSourceDriveId(sourceDrive.getDriveId());

		doReturn(null).when(service).getDrive(anyInt());
		doReturn("").when(service).recordException(any(ActionAudit.class), any(Exception.class));
		mockAudit(spec, service, "rename");

		Response result = service.rename(spec, mockedRequest, null);
		assertNotNull(result);
		assertEquals(BaseService.INTERNAL_SERVER_ERROR, result.getStatus());
		assertEquals(500, result.getStatus());
	}

	@Test
	public void testTransferMissingDestProvider() throws IOException {
		HttpServletRequest mockedRequest = Mockito.mock(HttpServletRequest.class);
		StringBuffer url = new StringBuffer("http://someurl.com");
		when(mockedRequest.getRequestURL()).thenReturn(url);
		when(mockedRequest.getUserPrincipal()).thenReturn(() -> "userName");

		Drive sourceDrive = initializeWindowsTestDrive();
		int sourceDriveId = sourceDrive.getDriveId();

		TransferSpec spec = buildLocalWindowsFileTransferSpec();
		spec.setSourceDriveId(sourceDriveId);
		spec.setDestDriveId(666);

		DriveService service = new DriveService();
		DriveService serviceSpy = Mockito.spy(service);
		doReturn(sourceDrive).when(serviceSpy).getDrive(sourceDriveId);
		doReturn(null).when(serviceSpy).getDrive(666);
		mockAudit(spec, serviceSpy, "copy");
		doReturn(true).when(serviceSpy)
				.assertDriveItemAccess(anyString(), any(Drive.class), any(AccessLevel.class));
		Mockito.doNothing().when(serviceSpy).updateAction(any(ActionAudit.class));

		Response result = serviceSpy.transfer(spec, mockedRequest, null);
		assertNotNull(result);
		assertEquals(BaseService.INTERNAL_SERVER_ERROR, result.getStatus());
	}

	@Test
	public void testTransferMissingDestPath() throws IOException {
		HttpServletRequest mockedRequest = Mockito.mock(HttpServletRequest.class);
		StringBuffer url = new StringBuffer("http://someurl.com");
		when(mockedRequest.getRequestURL()).thenReturn(url);
		when(mockedRequest.getUserPrincipal()).thenReturn(() -> "userName");

		Drive sourceDrive = initializeWindowsTestDrive();
		int sourceDriveId = sourceDrive.getDriveId();

		TransferSpec spec = buildLocalWindowsFileTransferSpec();
		spec.setSourceDriveId(sourceDriveId);
		spec.setDestDriveId(sourceDriveId);
		spec.setDestPath(null);

		DriveService serviceSpy = Mockito.spy(DriveService.class);
		doReturn(sourceDrive).when(serviceSpy).getDrive(sourceDriveId);
		doReturn(null).when(serviceSpy).recordException(any(ActionAudit.class), any(Exception.class), any(Long.class));
		mockAudit(spec, serviceSpy, "copy");

		Response result =
				serviceSpy.transfer(spec, mockedRequest, null);
		assertNotNull(result);
		assertEquals(BaseService.INTERNAL_SERVER_ERROR, result.getStatus());
	}

	@Test
	public void testTransferMissingSourcePath() throws IOException {
		HttpServletRequest mockedRequest = Mockito.mock(HttpServletRequest.class);
		StringBuffer url = new StringBuffer("http://someurl.com");
		when(mockedRequest.getRequestURL()).thenReturn(url);
		when(mockedRequest.getUserPrincipal()).thenReturn(() -> "userName");

		Drive sourceDrive = initializeWindowsTestDrive();
		int sourceDriveId = sourceDrive.getDriveId();

		TransferSpec spec = buildLocalWindowsFileTransferSpec();
		spec.setSourceDriveId(sourceDriveId);
		spec.setDestDriveId(sourceDriveId);
		spec.setDestPath("a");
		spec.setSourcePath(null);

		DriveService serviceSpy = Mockito.spy(DriveService.class);
		doReturn(sourceDrive).when(serviceSpy).getDrive(sourceDriveId);
		mockAudit(spec, serviceSpy, "copy");
		doReturn(null).when(serviceSpy).recordException(any(ActionAudit.class), any(Exception.class), any(Long.class));

		Response result =
				serviceSpy.transfer(spec, mockedRequest, null);
		assertNotNull(result);
		assertEquals(BaseService.INTERNAL_SERVER_ERROR, result.getStatus());
	}

	@Test
	public void testTransferSameSourceDest() throws IOException {
		HttpServletRequest mockedRequest = Mockito.mock(HttpServletRequest.class);
		StringBuffer url = new StringBuffer("http://someurl.com");
		when(mockedRequest.getRequestURL()).thenReturn(url);

		Drive sourceDrive = initializeWindowsTestDrive();
		int sourceDriveId = sourceDrive.getDriveId();

		TransferSpec spec = buildLocalWindowsFileTransferSpec();
		spec.setSourceDriveId(sourceDriveId);
		spec.setDestDriveId(sourceDriveId);
		spec.setDestPath("a");
		spec.setSourcePath("a");

		DriveService service = new DriveService();
		DriveService serviceSpy = Mockito.spy(service);
		doReturn(sourceDrive).when(serviceSpy).getDrive(sourceDriveId);
		doReturn("").when(serviceSpy).recordException(any(ActionAudit.class), any(Exception.class));
		mockAudit(spec, serviceSpy, "copy");

		Response result =
				serviceSpy.transfer(spec, mockedRequest, null);
		assertNotNull(result);
		assertEquals(BaseService.INTERNAL_SERVER_ERROR, result.getStatus());
	}

	@Test
	public void testTransferIllegitMove() throws IOException {
		HttpServletRequest mockedRequest = Mockito.mock(HttpServletRequest.class);
		StringBuffer url = new StringBuffer("http://someurl.com");
		when(mockedRequest.getRequestURL()).thenReturn(url);
		when(mockedRequest.getUserPrincipal()).thenReturn(() -> "userName");

		Drive sourceDrive = initializeWindowsTestDrive();
		int sourceDriveId = sourceDrive.getDriveId();

		TransferSpec spec = buildLocalWindowsFileTransferSpec();
		spec.setSourceDriveId(sourceDriveId);
		spec.setDestDriveId(sourceDriveId);
		spec.setSourcePath("a/");
		spec.setDestPath("a/b");
		spec.setRemoveSource(true);

		DriveService serviceSpy = Mockito.spy(DriveService.class);
		doReturn(sourceDrive).when(serviceSpy).getDrive(sourceDriveId);
		Mockito.doNothing().when(serviceSpy).updateAction(any(ActionAudit.class));

		mockAudit(spec, serviceSpy, "move");
		mockAudit(spec, serviceSpy, "move", ERROR, "a/ cannot be moved to a/b", "");

		Response result =
				serviceSpy.transfer(spec, mockedRequest, null);
		assertNotNull(result);
		assertEquals(BaseService.INTERNAL_SERVER_ERROR, result.getStatus());
		assertTrue(result.getEntity() instanceof ActionAuditResponse);
	}

	@Test
	public void testGetChunkSize() {
		assertEquals(DriveService.DEFAULT_UPLOAD_PART_SIZE,
					driveService.getChunkSize(0));
		assertEquals(DriveService.DEFAULT_UPLOAD_PART_SIZE,
					driveService.getChunkSize(1));
		assertEquals(DriveService.DEFAULT_UPLOAD_PART_SIZE,
				driveService.getChunkSize(DriveService.MAX_SIZE_TRANSFERRABLE_USING_DEFAULTS));
		assertEquals(2 * DriveService.DEFAULT_UPLOAD_PART_SIZE,
				driveService.getChunkSize(DriveService.MAX_SIZE_TRANSFERRABLE_USING_DEFAULTS + 1));
		assertEquals(2 * DriveService.DEFAULT_UPLOAD_PART_SIZE,
				driveService.getChunkSize(DriveService.MAX_SIZE_TRANSFERRABLE_USING_DEFAULTS * 2));
	}
}
