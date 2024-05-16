package com.spinsys.mdaca.storage.explorer.rest;

import com.spinsys.mdaca.storage.explorer.io.BasicFile;
import com.spinsys.mdaca.storage.explorer.io.FileUtil;
import com.spinsys.mdaca.storage.explorer.model.enumeration.AccessLevel;
import com.spinsys.mdaca.storage.explorer.model.DriveItem;
import com.spinsys.mdaca.storage.explorer.model.enumeration.DriveType;
import com.spinsys.mdaca.storage.explorer.model.enumeration.UserRole;
import com.spinsys.mdaca.storage.explorer.model.http.ChunkMetadata;
import com.spinsys.mdaca.storage.explorer.model.http.DownloadSpec;
import com.spinsys.mdaca.storage.explorer.model.http.DriveItemListSpec;
import com.spinsys.mdaca.storage.explorer.model.http.FileLocationSpec;
import com.spinsys.mdaca.storage.explorer.model.http.TransferSpec;
import com.spinsys.mdaca.storage.explorer.persistence.ActionAudit;
import com.spinsys.mdaca.storage.explorer.persistence.Drive;
import com.spinsys.mdaca.storage.explorer.persistence.DriveSecurityRule;
import com.spinsys.mdaca.storage.explorer.provider.StorageProvider;
import com.spinsys.mdaca.storage.explorer.model.exception.ExplorerException;
import com.spinsys.mdaca.storage.explorer.provider.StorageProviderFactory;
import org.apache.commons.io.FileUtils;
import org.apache.http.HttpStatus;
import org.jboss.resteasy.plugins.providers.multipart.InputPart;
import org.jboss.resteasy.plugins.providers.multipart.MultipartFormDataInputImpl;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import javax.persistence.EntityManager;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

import static com.spinsys.mdaca.storage.explorer.io.PathProcessor.GUI_SEP;
import static com.spinsys.mdaca.storage.explorer.io.PathProcessor.convertToUnixStylePath;
import static com.spinsys.mdaca.storage.explorer.provider.AWSS3StorageProvider.ACCESS_KEY_PROPERTY_KEY;
import static com.spinsys.mdaca.storage.explorer.provider.AWSS3StorageProvider.ACCESS_SECRET_PROPERTY_KEY;
import static com.spinsys.mdaca.storage.explorer.provider.AWSS3StorageProvider.BUCKET_NAME_PROPERTY_KEY;
import static com.spinsys.mdaca.storage.explorer.provider.AWSS3StorageProvider.REGION_PROPERTY_KEY;
import static com.spinsys.mdaca.storage.explorer.provider.BasicStorageProvider.DRIVE_NAME_PROPERTY_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;

class DriveServiceIT {

	/** A directory containing archive files (tar, zip, gzip, etc.). */
	private static final String ARCHIVE_DIR = "archiveFiles/";

	/** A small tar file. */
	private static final String SAMPLES_TAR = ARCHIVE_DIR + "samples.tar";

	/** A file name for a Windows dir that should already exist. */
	private static final String PREEXISTING_WINDOWS_DIR = "Temp";

	/** A file name for a Windows dir that should already exist. */
	private static final String PREEXISTING_WINDOWS_SUBDIR = "Temp/csv-dir";

	/** A small (< 1 kb) Windows file that exists in test/resources. */
	private static final String WINDOWS_FILE_SMALL = "sizedFiles/stateCodes_u1kb.csv";

	/** An ~2 MB Windows file that exists in test/resources. */
	private static final String WINDOWS_FILE_2MB = "sizedFiles/MDACA_CloudStorage_UserGuide_1.0_o2mb.docx";

	/** A file name for an S3 file that should already exist. */
	private static final String PREEXISTING_S3_FILE = "keithSaysLeaveMeAlone.txt";

	/** A file name for an existing Windows dir to contain uploads. */
	private static final String UPLOADS_DIR = "C:/Temp/Uploads";

    private static final String SAMPLE_FOLDER_PATH = "sampleFolder/";

    private static final Logger logger =
            Logger.getLogger("com.spinsys.mdaca.storage.explorer.rest.DriveServiceIT");

	/** Delete any old leftover testing artifacts
	    produced from this test, should they exist */
    @BeforeAll
	public static void cleanup() throws IOException {
		String winSubdir2 = PREEXISTING_WINDOWS_SUBDIR + "_2";

		try {
			FileUtils.deleteDirectory(new File(winSubdir2));
		} catch (Exception e) {
			logger.log(Level.WARNING, "Unable to delete " + winSubdir2, e);
		}
	}

	private DriveSecurityRule initializeSecurityRule(String roleName, String regex, boolean doExclude) {
		DriveSecurityRule rule = new DriveSecurityRule();
		rule.setRoleName(roleName);
		rule.setRuleText(regex);
		rule.setExclude(doExclude);
		rule.setAccessLevel("D");
		return rule;
	}

	public Drive initializeWindowsTestDrive() {
		Drive drive = new Drive();
		drive.setDisplayName("DriveServiceIT Windows Test Drive");
		drive.setDriveType(DriveType.Windows);
		drive.setDriveId(2);
		drive.addPropertyValue(DRIVE_NAME_PROPERTY_KEY, "C");
		DriveSecurityRule rule = initializeSecurityRule("", ".*", false);
		List<DriveSecurityRule> rules = new ArrayList<>();
		rules.add(rule);
		drive.setSecurityRules(rules);
		return drive;
	}

	public Drive initializeS3TestDrive() {
		Drive drive = new Drive();
		drive.setDisplayName("DriveServiceIT S3 Test Drive");
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

	TransferSpec buildLocalWindowsFileCopySpec() throws IOException {
		File sourceFile = FileUtil.getResourceFile(WINDOWS_FILE_SMALL);
		// The file name without the preceding directory info
		String fileName = sourceFile.getCanonicalPath();

		TransferSpec spec = new TransferSpec();
		spec.setSourceDriveId(2);
		spec.setSourcePath(convertToUnixStylePath(fileName));
		spec.setDestDriveId(2);
		spec.setDestPath(convertToUnixStylePath(fileName + "_2"));
		spec.setRemoveSource(false);
		return spec;
	}

	DownloadSpec buildLocalWindowsDownloadSpec() throws IOException {
		File sourceFile = FileUtil.getResourceFile("csvFiles/medications.csv");
		// The file name without the preceding directory info
		String fileName = sourceFile.getCanonicalPath();

		DownloadSpec spec = new DownloadSpec();
		spec.setDriveId(2);
		spec.setPath(convertToUnixStylePath(fileName));
		spec.setTopLines(50);
		spec.setBottomLines(10);
		return spec;
	}

	FileLocationSpec buildS3DirLocationSpec() {
		FileLocationSpec spec = new FileLocationSpec();
		spec.setDriveId(1);
		spec.setPath(SAMPLE_FOLDER_PATH);
		return spec;
	}

	FileLocationSpec buildExtractLocationSpec() {
		FileLocationSpec spec = new FileLocationSpec();
		spec.setDriveId(1);
		// TODO make less fragile
		spec.setPath(SAMPLES_TAR);
		return spec;
	}

	DriveItemListSpec buildDeleteSpec() {
		DriveItemListSpec spec = new DriveItemListSpec();

		DriveItem item = new DriveItem();
		item.setDriveId(1);
		// TODO make less fragile
		// The tar file will have been extracted into a directory
		// with the same name as the tar, minus the extension
		String untarredRoot = SAMPLES_TAR.replace(".tar", "");
		item.setPath(untarredRoot);

		spec.getDriveItems().add(item);
		return spec;
	}

	TransferSpec buildLocalWindowsDirCopySpec() {
		TransferSpec spec = new TransferSpec();
		spec.setSourceDriveId(2);
		spec.setSourcePath(PREEXISTING_WINDOWS_SUBDIR);
		spec.setDestDriveId(2);
		spec.setDestPath(PREEXISTING_WINDOWS_SUBDIR + "_2");
		spec.setRemoveSource(false);
		return spec;
	}

	TransferSpec buildS3toWindowsFileCopySpec() {
		TransferSpec spec = new TransferSpec();
		spec.setSourceDriveId(1);
		spec.setSourcePath(PREEXISTING_S3_FILE);
		spec.setDestDriveId(2);
		spec.setDestPath(PREEXISTING_WINDOWS_DIR + "\\" + PREEXISTING_S3_FILE + "_2");
		spec.setRemoveSource(false);
		return spec;
	}

	TransferSpec buildRemoteS3FileTransferSpec() {
		TransferSpec spec = new TransferSpec();
		spec.setSourceDriveId(1);
		spec.setSourcePath(PREEXISTING_S3_FILE);
//		spec.setSourcePath(PREEXISTING_S3_BIG_FILE);
		spec.setDestDriveId(1);
		spec.setDestPath(PREEXISTING_S3_FILE + "_2");
//		spec.setDestPath(PREEXISTING_S3_BIG_FILE + "_2");
		spec.setRemoveSource(false);
		return spec;
	}


	@Test
	@Tag("integration")
	public void testGetRelativePathSame() throws ExplorerException {
		StorageProvider provider =
			StorageProviderFactory.getProvider(DriveType.Windows);
		DriveService service = new DriveService();
		String relPath = service.getRelativePath(provider, "c:\\a", "c:\\a");
		assertEquals("", relPath);
	}
	
	@Test
	@Tag("integration")
	public void testGetRelativePathWinSub1() throws ExplorerException {
		StorageProvider provider =
				StorageProviderFactory.getProvider(DriveType.Windows);
		DriveService service = new DriveService();
		String relPath = service.getRelativePath(provider, "c:\\a", "c:\\a\\b");
		assertEquals("b", relPath);
	}
	
	@Test
	@Tag("integration")
	public void testGetRelativePathWinSub2() throws ExplorerException {
		StorageProvider provider =
				StorageProviderFactory.getProvider(DriveType.Windows);
		DriveService service = new DriveService();
		String relPath = service.getRelativePath(provider, "c:\\a", "c:\\a\\b\\c");
		assertEquals("b\\c", relPath);
	}
	
	@Test
	@Tag("integration")
	public void testRenameFile() throws Exception {
		HttpServletRequest mockedRequest = Mockito.mock(HttpServletRequest.class);
		StringBuffer url = new StringBuffer("http://someurl.com");
		when(mockedRequest.getRequestURL()).thenReturn(url);

		DriveService service = new DriveService();
		DriveService serviceSpy = Mockito.spy(service);

		// Both source and destination are S3
		Drive sourceDrive = initializeS3TestDrive();
		Drive destDrive = sourceDrive;
		int sourceDriveId = sourceDrive.getDriveId();
		int destDriveId = destDrive.getDriveId();

		TransferSpec spec = buildRemoteS3FileTransferSpec();
		String sourcePath = spec.getSourcePath();
		String destPath = spec.getDestPath();
		spec.setSourceDriveId(sourceDrive.getDriveId());

		EntityManager manager = Mockito.mock(EntityManager.class);
		when(manager.createQuery(anyString())).thenReturn(null);
		
		ActionAudit action = new ActionAudit();
		Mockito.doReturn(sourceDrive).when(serviceSpy).getDrive(sourceDriveId);
		Mockito.doReturn(destDrive).when(serviceSpy).getDrive(destDriveId);
		Mockito.doReturn(action).when(serviceSpy)	// 1st rename audit
			.auditAction("rename", sourcePath, sourceDriveId, ActionAudit.PENDING, destPath, destDriveId, null, null);
		Mockito.doReturn(action).when(serviceSpy)	// 2nd rename audit
			.auditAction("rename", destPath, sourceDriveId, ActionAudit.PENDING, sourcePath, destDriveId, null, null);
		Mockito.doReturn(true).when(serviceSpy)
			.assertDriveItemAccess(anyString(), any(Drive.class), any(AccessLevel.class));
		Mockito.doNothing().when(serviceSpy).updateAction(any(ActionAudit.class));

		Response result = serviceSpy.rename(spec, mockedRequest, null);
		assertNotNull(result);
		assertEquals(BaseService.SUCCESS_STATUS, result.getStatus());
		
		StorageProvider destProvider =
				StorageProviderFactory.getProvider(destDrive.getDriveType(), mockedRequest);
		assertTrue(destProvider.exists(destDrive, destPath));
		
		// restore original state by renaming
		// switch the source and destination
		spec.setDestPath(sourcePath);
		spec.setSourcePath(destPath);
		result = serviceSpy.rename(spec, mockedRequest, null);
		assertNotNull(result);
		assertEquals(BaseService.SUCCESS_STATUS, result.getStatus());
	}


	@Test
	@Tag("integration")
	public void testTransferOneFileDestFileAlreadyExists() throws Exception {
		HttpServletRequest mockedRequest = Mockito.mock(HttpServletRequest.class);
		StringBuffer url = new StringBuffer("http://someurl.com");
		when(mockedRequest.getRequestURL()).thenReturn(url);
		
		DriveService service = new DriveService();
		DriveService serviceSpy = Mockito.spy(service);
		ActionAudit audit = new ActionAudit();
		Mockito.doReturn(audit).when(serviceSpy)
			.auditAction(anyString(), anyString(), anyInt(), anyString());
		Mockito.doNothing().when(serviceSpy).updateAction(any(ActionAudit.class));

		Drive sourceDrive = initializeWindowsTestDrive();
		Drive destDrive = sourceDrive;
		TransferSpec spec = buildLocalWindowsFileCopySpec();
		spec.setSourceDriveId(sourceDrive.getDriveId());
		// source and destination are the same - should cause exception
		spec.setDestPath(spec.getSourcePath());

		EntityManager manager = Mockito.mock(EntityManager.class);
		when(manager.createQuery(anyString())).thenReturn(null);

		DriveItem driveItem = new DriveItem();
		driveItem.setFileSize(85);

		assertThrows(FileAlreadyExistsException.class, () ->
				serviceSpy.transferOneFile(driveItem, spec, sourceDrive, destDrive, audit, mockedRequest)
		);

	}


	@Test
	@Tag("integration")
	public void testTransferOneFileCopyFileUsingLocal() throws Exception {
		HttpServletRequest mockedRequest = Mockito.mock(HttpServletRequest.class);
		StringBuffer url = new StringBuffer("http://someurl.com");
		when(mockedRequest.getRequestURL()).thenReturn(url);

		DriveService service = new DriveService();
		Drive sourceDrive = initializeS3TestDrive();
		Drive destDrive = initializeWindowsTestDrive();
		TransferSpec spec = buildS3toWindowsFileCopySpec();
		spec.setSourceDriveId(sourceDrive.getDriveId());

		EntityManager manager = Mockito.mock(EntityManager.class);
		when(manager.createQuery(anyString())).thenReturn(null);
		
		ActionAudit audit = new ActionAudit();
		DriveItem driveItem = new DriveItem();
		driveItem.setFileSize(85);
		service.transferOneFile(driveItem, spec, sourceDrive, destDrive,
				audit, mockedRequest);

		StorageProvider destProvider =
				StorageProviderFactory.getProvider(destDrive.getDriveType(), mockedRequest);
		String destPath = spec.getDestPath();
		assertTrue(destProvider.exists(destDrive, destPath));
		
		// restore original state by deleting the uploaded file
		try {
			destProvider.delete(destDrive, destPath);
		} catch (Exception e) {
			// ignore - failing to clean up doesn't ruin the test
		}
	}

	@Test
	@Tag("integration")
	public void testTransferOneFileCopyFileUsingRemote() throws Exception {
		HttpServletRequest mockedRequest = Mockito.mock(HttpServletRequest.class);
		StringBuffer url = new StringBuffer("http://someurl.com");
		when(mockedRequest.getRequestURL()).thenReturn(url);

		DriveService service = new DriveService();
		
		// Both source and destination are S3
		Drive sourceDrive = initializeS3TestDrive();
		Drive destDrive = sourceDrive;
		TransferSpec spec = buildRemoteS3FileTransferSpec();
		spec.setSourceDriveId(sourceDrive.getDriveId());

		EntityManager manager = Mockito.mock(EntityManager.class);
		when(manager.createQuery(anyString())).thenReturn(null);
		
		ActionAudit audit = new ActionAudit();
		DriveItem driveItem = new DriveItem();
		driveItem.setFileSize(85);

		service.transferOneFile(driveItem, spec, sourceDrive, destDrive,
				audit, mockedRequest);

		StorageProvider destProvider =
				StorageProviderFactory.getProvider(destDrive.getDriveType(), mockedRequest);
		String destPath = spec.getDestPath();
		assertTrue(destProvider.exists(destDrive, destPath));
		
		// restore original state by deleting the uploaded file
		try {
			destProvider.delete(destDrive, destPath);
		} catch (Exception e) {
			// ignore - failing to clean up doesn't ruin the test
		}
	}

	@Test
	@Tag("integration")
	public void testTransferOneFileCopyFile() throws Exception {
		HttpServletRequest mockedRequest = Mockito.mock(HttpServletRequest.class);
		StringBuffer url = new StringBuffer("http://someurl.com");
		when(mockedRequest.getRequestURL()).thenReturn(url);

		DriveService service = Mockito.spy(new DriveService());
		Drive sourceDrive = initializeWindowsTestDrive();
		Drive destDrive = sourceDrive;

		TransferSpec spec = buildLocalWindowsFileCopySpec();
		spec.setSourceDriveId(sourceDrive.getDriveId());
		spec.setRemoveSource(false); // copy

		EntityManager manager = Mockito.mock(EntityManager.class);
		when(manager.createQuery(anyString())).thenReturn(null);
		Mockito.doReturn(new ActionAudit()).when(service)
				.auditAction("copy", spec.getSourcePath(), spec.getSourceDriveId(), "P", spec.getDestPath(), spec.getDestDriveId(), null, null);
		Mockito.doReturn(true).when(service)
				.assertDriveItemAccess(anyString(), any(Drive.class), any(AccessLevel.class));
		Mockito.doNothing().when(service).updateAction(any(ActionAudit.class));

		String sourcePath = spec.getSourcePath();
		File sourceFile = new File(sourcePath);
		ActionAudit audit = new ActionAudit();
		DriveItem driveItem = new DriveItem();
		driveItem.setFileSize(85);

		service.transferOneFile(driveItem, spec, sourceDrive, destDrive,
				audit, mockedRequest);

		StorageProvider destProvider =
				StorageProviderFactory.getProvider(destDrive.getDriveType(), mockedRequest);
		String destPath = spec.getDestPath();
		assertTrue(destProvider.exists(destDrive, destPath));

		StorageProvider sourceProvider =
				StorageProviderFactory.getProvider(sourceDrive.getDriveType(), mockedRequest);
		assertTrue(sourceProvider.exists(sourceDrive, sourcePath));
		assertTrue(FileUtils.contentEquals(sourceFile, new File(destPath)));

		// restore original state by deleting the uploaded file
		try {
			destProvider.delete(destDrive, destPath);
		} catch (Exception e) {
			// ignore - failing to clean up doesn't ruin the test
		}
	}

	@Test
	@Tag("integration")
	public void testTransferCopyFileS3Win() throws Exception {
		HttpServletRequest mockedRequest = Mockito.mock(HttpServletRequest.class);
		StringBuffer url = new StringBuffer("http://someurl.com");
		when(mockedRequest.getRequestURL()).thenReturn(url);
		
		Drive sourceDrive = initializeS3TestDrive();
		Drive destDrive = initializeWindowsTestDrive();;
		int sourceDriveId = sourceDrive.getDriveId();
		int destDriveId = destDrive.getDriveId();

		TransferSpec spec = buildS3toWindowsFileCopySpec();
		spec.setSourceDriveId(sourceDriveId);
		spec.setDestDriveId(destDriveId);
		spec.setRemoveSource(false);
		String sourcePath = spec.getSourcePath();
		String destPath = spec.getDestPath();
		
		ActionAudit action = new ActionAudit();
	
		DriveService service = new DriveService();
		DriveService serviceSpy = Mockito.spy(service);
		
		Mockito.doReturn(sourceDrive).when(serviceSpy).getDrive(sourceDriveId);
		Mockito.doReturn(destDrive).when(serviceSpy).getDrive(destDriveId);
		Mockito.doReturn(action).when(serviceSpy)
			.auditAction("copy", sourcePath, sourceDriveId, "P", destPath, spec.getDestDriveId(), null, null);
		Mockito.doReturn(true).when(serviceSpy)
			.assertDriveItemAccess(anyString(), any(Drive.class), any(AccessLevel.class));
		Mockito.doNothing().when(serviceSpy).updateAction(any(ActionAudit.class));

		Response result = serviceSpy.transfer(spec, mockedRequest, null);

		assertNotNull(result);
		assertEquals(BaseService.SUCCESS_STATUS, result.getStatus());
		
		StorageProvider destProvider =
				StorageProviderFactory.getProvider(destDrive.getDriveType(), mockedRequest);
		String origDestPath = spec.getDestPath();
		assertTrue(destProvider.exists(destDrive, origDestPath));

		StorageProvider sourceProvider =
				StorageProviderFactory.getProvider(sourceDrive.getDriveType(), mockedRequest);
		String origSourcePath = spec.getSourcePath();
		assertTrue(sourceProvider.exists(sourceDrive, origSourcePath));
		
		// restore original state by deleting the uploaded file
		try {
			destProvider.delete(destDrive, destPath);
		} catch (Exception e) {
			// ignore - failing to clean up doesn't ruin the test
		}
	}

	/**
	 * Tests copying a file from a Windows to Windows
	 * @throws Exception
	 */
	@Test
	@Tag("integration")
	public void testTransferCopyFileWinWin() throws Exception {
		HttpServletRequest mockedRequest = Mockito.mock(HttpServletRequest.class);
		StringBuffer url = new StringBuffer("http://someurl.com");
		when(mockedRequest.getRequestURL()).thenReturn(url);
		
		Drive sourceDrive = initializeWindowsTestDrive();
		Drive destDrive = sourceDrive;
		int sourceDriveId = sourceDrive.getDriveId();
		int destDriveId = destDrive.getDriveId();

		TransferSpec spec = buildLocalWindowsFileCopySpec();
		spec.setSourceDriveId(sourceDriveId);
		spec.setDestDriveId(destDriveId);
		spec.setRemoveSource(false);
		String sourcePath = spec.getSourcePath();
		String destPath = spec.getDestPath();
		
		ActionAudit action = new ActionAudit();

		DriveService service = new DriveService();
		DriveService serviceSpy = Mockito.spy(service);
		
		Mockito.doReturn(sourceDrive).when(serviceSpy).getDrive(sourceDriveId);
		Mockito.doReturn(destDrive).when(serviceSpy).getDrive(destDriveId);
		Mockito.doReturn(action).when(serviceSpy)
			.auditAction("copy", sourcePath, sourceDriveId, "P", destPath, spec.getDestDriveId(), null, null);
		Mockito.doReturn(true).when(serviceSpy)
			.assertDriveItemAccess(anyString(), any(Drive.class), any(AccessLevel.class));
		Mockito.doNothing().when(serviceSpy).updateAction(any(ActionAudit.class));
		mockAudit(spec, serviceSpy, "copy");

		Response result = serviceSpy.transfer(spec, mockedRequest, null);

		assertNotNull(result);
		assertEquals(BaseService.SUCCESS_STATUS, result.getStatus());
		
		StorageProvider destProvider =
				StorageProviderFactory.getProvider(destDrive.getDriveType(), mockedRequest);
		String origDestPath = spec.getDestPath();
		assertTrue(destProvider.exists(destDrive, origDestPath));

		StorageProvider sourceProvider =
				StorageProviderFactory.getProvider(sourceDrive.getDriveType(), mockedRequest);
		assertTrue(sourceProvider.exists(sourceDrive, sourcePath));
		
		assertTrue(FileUtils.contentEquals(new File(sourcePath), new File(destPath)));

		// restore original state by deleting the copy
		destProvider.delete(destDrive, destPath);
	}

	@Test
	@Tag("integration")
	public void testAuthorizationFailOnTransfer() {
		HttpServletRequest mockedRequest = Mockito.mock(HttpServletRequest.class);
		StringBuffer url = new StringBuffer("http://www.besturlever.com");
		when(mockedRequest.getRequestURL()).thenReturn(url);

		TransferSpec spec = buildRemoteS3FileTransferSpec();
		int driveId = spec.getSourceDriveId();

		DriveService serviceSpy = Mockito.spy(DriveService.class);
//		doReturn(new ArrayList<>()).when(serviceSpy).getDrivesByDriveId(driveId, any(EntityManager.class));
		doReturn("").when(serviceSpy).recordException(any(ActionAudit.class), any(Exception.class));
		doReturn(UserRole.ADMIN).when(serviceSpy).getUserRole();
		mockAudit(spec, serviceSpy, "copy");
		Response response = serviceSpy.transfer(spec, mockedRequest, null);

		assertNotNull(response);
		assertEquals(HttpStatus.SC_INTERNAL_SERVER_ERROR, response.getStatus());
	}

	@Test
	@Tag("integration")
	public void testTransferCopyDirWinWin() throws Exception {
		HttpServletRequest mockedRequest = Mockito.mock(HttpServletRequest.class);
		StringBuffer url = new StringBuffer("http://someurl.com");
		when(mockedRequest.getRequestURL()).thenReturn(url);
		
		Drive sourceDrive = initializeWindowsTestDrive();
		Drive destDrive = sourceDrive;
		int sourceDriveId = sourceDrive.getDriveId();
		int destDriveId = destDrive.getDriveId();

		TransferSpec spec = buildLocalWindowsDirCopySpec();
		spec.setSourceDriveId(sourceDriveId);
		spec.setDestDriveId(destDriveId);
		spec.setRemoveSource(false);
		String sourcePath = spec.getSourcePath();
		String destPath = spec.getDestPath();
		
		ActionAudit action = new ActionAudit();
	
		DriveService service = new DriveService();
		DriveService serviceSpy = Mockito.spy(service);

		StorageProvider destProvider =
				StorageProviderFactory.getProvider(destDrive.getDriveType(), mockedRequest);

		StorageProvider sourceProvider =
				StorageProviderFactory.getProvider(sourceDrive.getDriveType(), mockedRequest);
		String origSourcePath = spec.getSourcePath();
		assertTrue(sourceProvider.exists(sourceDrive, origSourcePath));

		Mockito.doReturn(sourceDrive).when(serviceSpy).getDrive(sourceDriveId);
		Mockito.doReturn(destDrive).when(serviceSpy).getDrive(destDriveId);
		Mockito.doReturn(action).when(serviceSpy)
			.auditAction("copy", sourcePath, sourceDriveId, "P", destPath, spec.getDestDriveId(), null, null);
		Mockito.doReturn(true).when(serviceSpy)
			.assertDriveItemAccess(anyString(), any(Drive.class), any(AccessLevel.class));
		Mockito.doNothing().when(serviceSpy).updateAction(any(ActionAudit.class));

		if (destProvider.exists(destDrive, destPath)) {
			destProvider.deleteDirectory(destDrive, destPath);
		}

		Response result = serviceSpy.transfer(spec, mockedRequest, null);

		assertNotNull(result);
		assertEquals(BaseService.SUCCESS_STATUS, result.getStatus());
		String origDestPath = spec.getDestPath();
		assertTrue(destProvider.exists(destDrive, origDestPath));

		// TODO more assertions
		
		// restore original state by deleting the copy
		try {
			destProvider.delete(destDrive, destPath);
		} catch (Exception e) {
			// ignore - failing to clean up doesn't ruin the test
		}
	}

	@Test
	@Tag("integration")
	public void testTransferCopyDirProviderIOException() throws Exception {
		HttpServletRequest mockedRequest = Mockito.mock(HttpServletRequest.class);
		StringBuffer url = new StringBuffer("http://someurl.com");
		when(mockedRequest.getRequestURL()).thenReturn(url);
		
		Drive sourceDrive = initializeWindowsTestDrive();
		Drive destDrive = sourceDrive;
		int sourceDriveId = sourceDrive.getDriveId();
		int destDriveId = destDrive.getDriveId();
		
		TransferSpec spec = buildLocalWindowsDirCopySpec();
		spec.setSourceDriveId(sourceDriveId);
		spec.setDestDriveId(destDriveId);
		spec.setRemoveSource(false);
		String sourcePath = spec.getSourcePath();
		String destPath = spec.getDestPath();
		
		ActionAudit action = new ActionAudit();
	
		DriveService service = new DriveService();
		DriveService serviceSpy = Mockito.spy(service);
		
		Mockito.doReturn(sourceDrive).when(serviceSpy).getDrive(sourceDriveId);
		Mockito.doReturn(destDrive).when(serviceSpy).getDrive(destDriveId);
		Mockito.doReturn(action).when(serviceSpy)
			.auditAction("copy", sourcePath, sourceDriveId, "P", destPath, spec.getDestDriveId(), null, null);
		Mockito.doReturn(true).when(serviceSpy)
			.assertDriveItemAccess(anyString(), any(Drive.class), any(AccessLevel.class));
		Mockito.doNothing().when(serviceSpy).updateAction(any(ActionAudit.class));

		// Make sure the destination directory already exists, so
		// an IOException will be thrown, resulting in an internal server error
//		File destDirectory = new File(destPath);
//		FileUtils.forceMkdir(destDirectory);
		StorageProvider destProvider = StorageProviderFactory.getProvider(destDrive.getDriveType());

		if (!destProvider.exists(destDrive, destPath)) {
			destProvider.mkdir(destDrive, destPath);
		}
		Response result = serviceSpy.transfer(spec, mockedRequest, null);

		assertNotNull(result);
		assertEquals(BaseService.INTERNAL_SERVER_ERROR, result.getStatus());
		
		StorageProvider sourceProvider =
				StorageProviderFactory.getProvider(sourceDrive.getDriveType(), mockedRequest);
		String origSourcePath = spec.getSourcePath();
		assertTrue(sourceProvider.exists(sourceDrive, origSourcePath));
		
		// clean up
		try {
			destProvider.deleteDirectory(destDrive, destPath);
		}
		catch (Exception e) {
			// ignore
		}
	}

	/**
	 * Test an upload of a small file to a Windows provider
	 * @throws Exception
	 */
	@Test
//	@Order(1000)	// testExtractTar will depend on an uploaded tar file
	@Tag("integration")
	public void testUploadChunkFileWinSmall() throws Exception {
		File sourceFile = FileUtil.getResourceFile(WINDOWS_FILE_SMALL);
		// The file name without the preceding directory info
		String fileName = sourceFile.getName();
		String destPath = UPLOADS_DIR + GUI_SEP + fileName;
		Drive destDrive = initializeWindowsTestDrive();

		HttpServletRequest mockedRequest = Mockito.mock(HttpServletRequest.class);
		StringBuffer url = new StringBuffer("http://someurl.com");
		when(mockedRequest.getRequestURL()).thenReturn(url);

		InputPart driveIdInputPart = Mockito.mock(InputPart.class);
		InputPart pathInputPart = Mockito.mock(InputPart.class);
		InputPart filePart = Mockito.mock(InputPart.class);
		MultivaluedMap<String, String> headers = Mockito.mock(MultivaluedMap.class);

		when(driveIdInputPart.getBodyAsString()).thenReturn("" + destDrive.getDriveId());
		when(pathInputPart.getBodyAsString()).thenReturn(UPLOADS_DIR);
		when(filePart.getBody(InputStream.class, null))
			.thenReturn(FileUtils.openInputStream(sourceFile));
		when(filePart.getHeaders()).thenReturn(headers);

		List<InputPart> inputPartList = new ArrayList<>();
		List<InputPart> pathList = new ArrayList<>();
		List<InputPart> filesList = new ArrayList<>();
		inputPartList.add(driveIdInputPart);
		pathList.add(pathInputPart);
		filesList.add(filePart);

		Map<String, List<InputPart>> mockedUploadForm = Mockito.mock(Map.class);
		when(mockedUploadForm.get("driveId")).thenReturn(inputPartList);
		when(mockedUploadForm.get("files")).thenReturn(filesList);
		when(mockedUploadForm.get("path")).thenReturn(pathList);

		ChunkMetadata mockMeta = Mockito.mock(ChunkMetadata.class);

		when(mockMeta.getFileName()).thenReturn(fileName);
		when(mockMeta.getChunkIndex()).thenReturn(0L);
		when(mockMeta.getTotalChunks()).thenReturn(1L);
		when(mockMeta.isEnd()).thenReturn(true);
		when(mockMeta.getFileUid()).thenReturn("_f1kb_" + System.currentTimeMillis());
		
		MultipartFormDataInputImpl mockedInput = Mockito.mock(MultipartFormDataInputImpl.class);
		when(mockedInput.getFormDataMap()).thenReturn(mockedUploadForm);

		DriveService service = new DriveService();
		DriveService serviceSpy = Mockito.spy(service);
		
		Mockito.doReturn(mockMeta).when(serviceSpy).getChunkMetadata(mockedUploadForm);
		Mockito.doReturn(destDrive).when(serviceSpy).getDrive(anyInt());
		Mockito.doReturn(true).when(serviceSpy)
			.assertDriveItemAccess(anyString(), any(Drive.class),
					any(AccessLevel.class));
		Mockito.doNothing().when(serviceSpy).updateAction(any(ActionAudit.class));
		ActionAudit action = new ActionAudit();
		Mockito.doReturn(action).when(serviceSpy)
			.auditAction(anyString(),
						anyString(), anyInt(), anyString());
		Mockito.doReturn(action).when(serviceSpy)
			.auditAction(anyString(),
					anyString(), anyInt(),
					anyString(), anyInt(),
					anyString());
		Mockito.doReturn(action).when(serviceSpy)
			.getActionAudit(anyString(),
					anyString(), anyInt(),
					anyString(), anyInt(),
					anyString());

		Response result = serviceSpy.uploadChunk(mockedInput, mockedRequest, null);

		assertNotNull(result);
		assertEquals(BaseService.SUCCESS_STATUS, result.getStatus());
		assertTrue(FileUtils.contentEquals(sourceFile, new File(destPath)));
		
		StorageProvider destProvider =
				StorageProviderFactory.getProvider(destDrive.getDriveType(), mockedRequest);

		// restore original state by deleting the uploaded file
		try {
			destProvider.delete(destDrive, destPath);
		} catch (Exception e) {
			// ignore - failing to clean up doesn't ruin the test
		}
	}

	@Test
	@Tag("integration")
	public void testPreviewSimple() throws Exception {
		HttpServletRequest mockedRequest = Mockito.mock(HttpServletRequest.class);
		StringBuffer url = new StringBuffer("http://someurl.com");
		when(mockedRequest.getRequestURL()).thenReturn(url);

		DriveService service = Mockito.spy(new DriveService());
		Drive sourceDrive = initializeWindowsTestDrive();

		DownloadSpec spec = buildLocalWindowsDownloadSpec();

		EntityManager manager = Mockito.mock(EntityManager.class);
		when(manager.createQuery(anyString())).thenReturn(null);
//		Mockito.doReturn(new ActionAudit()).when(service)
//				.auditAction(mockedRequest, "copy", spec.getSourcePath(), spec.getSourceDriveId(), "P", 0, spec.getDestPath(), spec.getDestDriveId(), null, null);
		Mockito.doReturn(true).when(service)
				.assertDriveItemAccess(anyString(), any(Drive.class), any(AccessLevel.class));
		Mockito.doNothing().when(service).updateAction(any(ActionAudit.class));

		String sourcePath = spec.getPath();
		File sourceFile = new File(sourcePath);
		ActionAudit audit = new ActionAudit();
		
		StorageProvider provider =
				StorageProviderFactory.getProvider(sourceDrive.getDriveType(), mockedRequest);

		BasicFile file = service.preview(provider, sourceDrive, sourceFile.length(), spec, audit);
		assertNotNull(file);
		String contents = file.contents();
		assertNotNull(contents);
		
		long expectedCount = spec.getTopLines() + spec.getBottomLines();
		long actualCount = 0;
		Path previewPath = Paths.get(file.getCanonicalPath());
		try (Stream<String> stream = Files.lines(previewPath, StandardCharsets.UTF_8)) {
		  actualCount = stream.count();
		}
		assertEquals(expectedCount, actualCount);
//		assertTrue(FileUtils.contentEquals(sourceFile, new File(destPath)));

		// restore original state by deleting the files
		try {
			file.delete();
		} catch (Exception e) {
			// ignore - failing to clean up doesn't ruin the test
		}
	}

	/**
	 * Test an upload of a small file to a Windows provider
	 * @throws Exception
	 */
	@Disabled("More logic needed in the test")
	@Test
	@Tag("integration")
	public void testUploadChunkFileWin2MB() throws Exception {
		File sourceFile = FileUtil.getResourceFile(WINDOWS_FILE_2MB);
		// The file name without the preceding directory info
		String fileName = sourceFile.getName();
		String destPath = UPLOADS_DIR + GUI_SEP + fileName;
		Drive destDrive = initializeWindowsTestDrive();

		HttpServletRequest mockedRequest = Mockito.mock(HttpServletRequest.class);
		StringBuffer url = new StringBuffer("http://someurl.com");
		when(mockedRequest.getRequestURL()).thenReturn(url);
		
		InputPart driveIdInputPart = Mockito.mock(InputPart.class);
		InputPart pathInputPart = Mockito.mock(InputPart.class);
		InputPart filePart = Mockito.mock(InputPart.class);
		MultivaluedMap<String, String> headers = Mockito.mock(MultivaluedMap.class);

		when(driveIdInputPart.getBodyAsString()).thenReturn("" + destDrive.getDriveId());
		when(pathInputPart.getBodyAsString()).thenReturn(UPLOADS_DIR);
		when(filePart.getBody(InputStream.class, null))
			.thenReturn(FileUtils.openInputStream(sourceFile));
		when(filePart.getHeaders()).thenReturn(headers);

		List<InputPart> inputPartList = new ArrayList<>();
		List<InputPart> pathList = new ArrayList<>();
		List<InputPart> filesList = new ArrayList<>();
		inputPartList.add(driveIdInputPart);
		pathList.add(pathInputPart);
		filesList.add(filePart);

		Map<String, List<InputPart>> mockedUploadForm = Mockito.mock(Map.class);
		when(mockedUploadForm.get("driveId")).thenReturn(inputPartList);
		when(mockedUploadForm.get("files")).thenReturn(filesList);
		when(mockedUploadForm.get("path")).thenReturn(pathList);
		
		ChunkMetadata mockMeta = Mockito.mock(ChunkMetadata.class);

		when(mockMeta.getFileName()).thenReturn(fileName);
		when(mockMeta.getChunkIndex()).thenReturn(0L, 1L, 2L, 3L);
		when(mockMeta.getTotalChunks()).thenReturn(3L); // 1 mb/chunk
		when(mockMeta.getFileUid()).thenReturn("_f2MB_" + System.currentTimeMillis());
		
		MultipartFormDataInputImpl mockedInput = Mockito.mock(MultipartFormDataInputImpl.class);
		when(mockedInput.getFormDataMap()).thenReturn(mockedUploadForm);
				
		DriveService service = new DriveService();
		DriveService serviceSpy = Mockito.spy(service);
		
		Mockito.doReturn(mockMeta).when(serviceSpy).getChunkMetadata(mockedUploadForm);
		Mockito.doReturn(destDrive).when(serviceSpy).getDrive(anyInt());
		Mockito.doReturn(true).when(serviceSpy)
			.assertDriveItemAccess(anyString(), any(Drive.class), any(AccessLevel.class));
		Mockito.doNothing().when(serviceSpy).updateAction(any(ActionAudit.class));
		ActionAudit action = new ActionAudit();
		Mockito.doReturn(action).when(serviceSpy)
			.auditAction(anyString(), anyString(), anyInt(), anyString());
//		this.auditAction(request, "uploadchunk", uploadPath, driveId, "P", 0);

		Response result = serviceSpy.uploadChunk(mockedInput, mockedRequest, null);

		assertNotNull(result);
		assertEquals(BaseService.SUCCESS_STATUS, result.getStatus());
		assertTrue(FileUtils.contentEquals(sourceFile, new File(destPath)));
		
		StorageProvider destProvider =
				StorageProviderFactory.getProvider(destDrive.getDriveType(), mockedRequest);

		// restore original state by deleting the uploaded file
		try {
			destProvider.delete(destDrive, destPath);
		} catch (Exception e) {
			// ignore - failing to clean up doesn't ruin the test
		}
	}

	@Test
	@Tag("integration")
    public void testMkdir() throws Exception {
		HttpServletRequest mockedRequest = Mockito.mock(HttpServletRequest.class);
		StringBuffer url = new StringBuffer("http://someurl.com");
		when(mockedRequest.getRequestURL()).thenReturn(url);

		DriveService service = new DriveService();
		DriveService serviceSpy = Mockito.spy(service);
		
		ActionAudit audit = new ActionAudit();
		Mockito.doReturn(audit).when(serviceSpy)
			.auditAction(anyString(), anyString(), anyInt(), anyString());
		Mockito.doNothing().when(serviceSpy)
			.updateAction(any(ActionAudit.class));
		Mockito.doReturn(true).when(serviceSpy)
			.assertDriveItemAccess(anyString(), any(Drive.class), any(AccessLevel.class));

		FileLocationSpec spec = buildS3DirLocationSpec();
		Drive drive = initializeS3TestDrive();
		Mockito.doReturn(drive).when(serviceSpy).getDrive(spec.getDriveId());
		StorageProvider provider =
				StorageProviderFactory.getProvider(drive.getDriveType(), mockedRequest);

		serviceSpy.mkdir(spec, mockedRequest, null);

		mockExists(serviceSpy, audit, spec);
        Response existsResponse = serviceSpy.exists(spec, mockedRequest, null);
		assertEquals(BaseService.SUCCESS_STATUS, existsResponse.getStatus());
		assertEquals(true, existsResponse.getEntity());
        assertTrue(provider.isDirectory(drive, SAMPLE_FOLDER_PATH));

        provider.deleteDirectory(drive, SAMPLE_FOLDER_PATH);
        existsResponse = serviceSpy.exists(spec, mockedRequest, null);
		assertEquals(BaseService.SUCCESS_STATUS, existsResponse.getStatus());
		assertEquals(false, existsResponse.getEntity());
        assertFalse(provider.exists(drive, SAMPLE_FOLDER_PATH));
    }

	@Test
	@Order(10000)	// testDeleteUntarredFiles depends on this
	@Tag("integration")
    public void testExtractTar() throws Exception {
		HttpServletRequest mockedRequest = Mockito.mock(HttpServletRequest.class);
		StringBuffer url = new StringBuffer("http://someurl.com");
		when(mockedRequest.getRequestURL()).thenReturn(url);

		DriveService service = new DriveService();
		DriveService serviceSpy = Mockito.spy(service);
		
		ActionAudit audit = new ActionAudit();
		Mockito.doReturn(audit).when(serviceSpy)
			.auditAction(anyString(), anyString(), anyInt(), anyString());
		Mockito.doNothing().when(serviceSpy)
			.updateAction(any(ActionAudit.class));
		Mockito.doReturn(true).when(serviceSpy)
				.assertDriveItemAccess(anyString(), any(Drive.class), any(AccessLevel.class));

		FileLocationSpec spec = buildExtractLocationSpec();
		Drive drive = initializeS3TestDrive();
		Mockito.doReturn(drive).when(serviceSpy).getDrive(spec.getDriveId());
		StorageProvider provider =
				StorageProviderFactory.getProvider(drive.getDriveType(), mockedRequest);

		serviceSpy.extractFile(spec, mockedRequest, null);
		// The contents of the tar file will be put into a
		// directory of the same name, minus the ".tar"
		String newDir = spec.getPath().replace(".tar", "");

		mockExists(serviceSpy, audit, spec);

        Response existsResponse = serviceSpy.exists(spec, mockedRequest, null);
		assertEquals(BaseService.SUCCESS_STATUS, existsResponse.getStatus());
		assertEquals(true, existsResponse.getEntity());
        assertTrue(provider.isDirectory(drive, newDir));
        
        // cleanup/state restoration will occur via testDeleteUntarredFiles()
    }

	@Test
	@Order(10001)	// depends on testExtractTar
	@Tag("integration")
    public void testDeleteUntarredFiles() throws Exception {
		HttpServletRequest mockedRequest = Mockito.mock(HttpServletRequest.class);
		StringBuffer url = new StringBuffer("http://someurl.com");
		when(mockedRequest.getRequestURL()).thenReturn(url);

		DriveService service = new DriveService();
		DriveService serviceSpy = Mockito.spy(service);
		
		ActionAudit audit = new ActionAudit();
		Mockito.doReturn(audit).when(serviceSpy)
			.auditAction(anyString(), anyString(), anyInt(), anyString());
		Mockito.doNothing().when(serviceSpy)
			.updateAction(any(ActionAudit.class));
		Mockito.doReturn(true).when(serviceSpy)
			.assertDriveItemAccess(anyString(), any(Drive.class), any(AccessLevel.class));

		DriveItemListSpec spec = buildDeleteSpec();
		Drive drive = initializeS3TestDrive();
		Mockito.doReturn(drive).when(serviceSpy).getDrive(spec.getDriveId());
		serviceSpy.delete(spec, mockedRequest, null);

		FileLocationSpec singleFileLocSpec = new FileLocationSpec();
		singleFileLocSpec.setDriveId(spec.getDriveId());
		singleFileLocSpec.setPath(spec.getDriveItems().get(0).getPath());

		mockExists(serviceSpy, audit, singleFileLocSpec);

		Response existsResponse = serviceSpy.exists(singleFileLocSpec, mockedRequest, null);
		assertEquals(BaseService.SUCCESS_STATUS, existsResponse.getStatus());
		assertEquals(false, existsResponse.getEntity());
    }

	private void mockExists(DriveService serviceSpy, ActionAudit audit, FileLocationSpec spec) {
		Mockito.doReturn(audit).when(serviceSpy)
				.auditAction("exists", spec.getPath(), spec.getDriveId(), "P", null, 0, null, null);
	}

	private void mockAudit(TransferSpec spec, DriveService serviceSpy, String action) {
		doReturn(new ActionAudit()).when(serviceSpy)
				.auditAction(action, spec.getSourcePath(), spec.getSourceDriveId(), "P",
						spec.getDestPath(), spec.getDestDriveId(), null, null);
	}

}
