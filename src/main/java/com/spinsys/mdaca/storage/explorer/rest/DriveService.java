package com.spinsys.mdaca.storage.explorer.rest;

import static com.spinsys.mdaca.storage.explorer.io.FileUtil.buildTempFile;
import static com.spinsys.mdaca.storage.explorer.io.FileUtil.deleteTempFile;
import static com.spinsys.mdaca.storage.explorer.io.PathProcessor.GUI_SEP;
import static com.spinsys.mdaca.storage.explorer.io.PathProcessor.addLastSlash;
import static com.spinsys.mdaca.storage.explorer.io.PathProcessor.getExtension;
import static com.spinsys.mdaca.storage.explorer.io.PathProcessor.getParentFolder;
import static com.spinsys.mdaca.storage.explorer.io.PathProcessor.isNewLine;
import static com.spinsys.mdaca.storage.explorer.io.PathProcessor.sameSourceAndDestination;
import static com.spinsys.mdaca.storage.explorer.model.enumeration.AccessLevel.Delete;
import static com.spinsys.mdaca.storage.explorer.model.enumeration.AccessLevel.Read;
import static com.spinsys.mdaca.storage.explorer.model.enumeration.UserRole.ADMIN;
import static com.spinsys.mdaca.storage.explorer.persistence.ActionAudit.ERROR;
import static com.spinsys.mdaca.storage.explorer.persistence.ActionAudit.PENDING;
import static com.spinsys.mdaca.storage.explorer.persistence.ActionAudit.UNAUTHORIZED;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.Principal;
import java.security.acl.Group;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

import javax.annotation.Resource;
import javax.enterprise.concurrent.ManagedExecutorService;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.persistence.EntityManager;
import javax.security.auth.Subject;
import javax.security.jacc.PolicyContext;
import javax.security.jacc.PolicyContextException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.transaction.UserTransaction;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.OPTIONS;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.avro.Schema;
import org.apache.commons.cli.MissingArgumentException;
import org.apache.commons.compress.compressors.z.ZCompressorInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.parquet.schema.MessageType;
import org.jboss.security.SimpleGroup;
import org.wildfly.security.auth.server.SecurityIdentity;
import org.wildfly.security.authz.Roles;
import org.jboss.resteasy.plugins.providers.multipart.InputPart;
import org.jboss.resteasy.plugins.providers.multipart.MultipartFormDataInput;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.primitives.Bytes;
import com.spinsys.mdaca.storage.explorer.bigdata.AVROFileProcessor;
import com.spinsys.mdaca.storage.explorer.bigdata.CSVFileProcessor;
import com.spinsys.mdaca.storage.explorer.bigdata.HiveConnector;
import com.spinsys.mdaca.storage.explorer.bigdata.HiveTableMaker;
import com.spinsys.mdaca.storage.explorer.bigdata.ParquetFileProcessor;
import com.spinsys.mdaca.storage.explorer.io.BasicFile;
import com.spinsys.mdaca.storage.explorer.io.FileUtil;
import com.spinsys.mdaca.storage.explorer.io.PathProcessor;
import com.spinsys.mdaca.storage.explorer.io.ThrottledInputStream;
import com.spinsys.mdaca.storage.explorer.model.DriveItem;
import com.spinsys.mdaca.storage.explorer.model.DriveQuery;
import com.spinsys.mdaca.storage.explorer.model.StorageClass;
import com.spinsys.mdaca.storage.explorer.model.dto.FolderMemoryUsageDTO;
import com.spinsys.mdaca.storage.explorer.model.enumeration.AccessLevel;
import com.spinsys.mdaca.storage.explorer.model.enumeration.DriveType;
import com.spinsys.mdaca.storage.explorer.model.enumeration.UserRole;
import com.spinsys.mdaca.storage.explorer.model.exception.ArchiveException;
import com.spinsys.mdaca.storage.explorer.model.exception.AuthorizationException;
import com.spinsys.mdaca.storage.explorer.model.exception.DisplayableException;
import com.spinsys.mdaca.storage.explorer.model.exception.DriveItemExistsException;
import com.spinsys.mdaca.storage.explorer.model.exception.ExplorerException;
import com.spinsys.mdaca.storage.explorer.model.exception.MissingInputException;
import com.spinsys.mdaca.storage.explorer.model.http.ActionAuditResponse;
import com.spinsys.mdaca.storage.explorer.model.http.ChunkMetadata;
import com.spinsys.mdaca.storage.explorer.model.http.ChunkResult;
import com.spinsys.mdaca.storage.explorer.model.http.DownloadSpec;
import com.spinsys.mdaca.storage.explorer.model.http.DriveItemListSpec;
import com.spinsys.mdaca.storage.explorer.model.http.ExternalTableSpec;
import com.spinsys.mdaca.storage.explorer.model.http.FileLocationSpec;
import com.spinsys.mdaca.storage.explorer.model.http.ListFileLocationSpec;
import com.spinsys.mdaca.storage.explorer.model.http.RestoreSpec;
import com.spinsys.mdaca.storage.explorer.model.http.StorageClassSpec;
import com.spinsys.mdaca.storage.explorer.model.http.TransferSpec;
import com.spinsys.mdaca.storage.explorer.persistence.ActionAudit;
import com.spinsys.mdaca.storage.explorer.persistence.Drive;
import com.spinsys.mdaca.storage.explorer.persistence.DriveSecurityRule;
import com.spinsys.mdaca.storage.explorer.persistence.DriveUser;
import com.spinsys.mdaca.storage.explorer.persistence.MemoryUsage;
import com.spinsys.mdaca.storage.explorer.provider.CloudStorageProvider;
import com.spinsys.mdaca.storage.explorer.provider.RestorableCloudStorageProvider;
import com.spinsys.mdaca.storage.explorer.provider.StorageProvider;
import com.spinsys.mdaca.storage.explorer.provider.StorageProviderFactory;
import com.spinsys.mdaca.storage.explorer.tasks.TransferTask;

import javassist.NotFoundException;
import net.sourceforge.spnego.SpnegoPrincipal;

@Path("drives")
public class DriveService extends BaseService {

	/** The size of each part (except the last),
	 *  when a file is uploaded in multiple chunks. */
	static final int DEFAULT_UPLOAD_PART_SIZE = 10 * 1_048_576; // 10 MB
	
	/** The maximum of parts/chunks that cab be used to
	 * transfer a single file. */
	static final int MAX_NUMBER_PARTS_PER_UPLOAD = 10_000;
	
	static final long MAX_SIZE_TRANSFERRABLE_USING_DEFAULTS =
			(long)MAX_NUMBER_PARTS_PER_UPLOAD * DEFAULT_UPLOAD_PART_SIZE; // 100 GB
	
	/** When this service started. */
	static final Date SERVICE_START_TIME = new Date();

	private static final Logger logger =
			Logger.getLogger("com.spinsys.mdaca.storage.explorer.rest.DriveService");

	public DriveService() {
		super();
	}

	public DriveService(EntityManager entityManager, UserTransaction utx) {
		super(entityManager, utx);
	}

	public List<AccessLevel> getDriveItemAccess(String path, Drive drive) {

		List<AccessLevel> accessLevels = new ArrayList<AccessLevel>();

		boolean hasRead = false;
		boolean hasCreate = false;
		boolean hasModify = false;
		boolean hasDelete = false;

		hasDelete = assertDriveItemAccess(path, drive, AccessLevel.Delete);

		if(hasDelete == false) {

			hasModify = assertDriveItemAccess(path, drive, AccessLevel.Modify);

			if(hasModify == false) {

				hasCreate = assertDriveItemAccess(path, drive, AccessLevel.Create);

				if(hasCreate == false) {

					hasRead = assertDriveItemAccess(path, drive, AccessLevel.Read);

					if(hasRead == true) {
						accessLevels.add(AccessLevel.Read);
					}

				} else {
					accessLevels.add(AccessLevel.Read);
					accessLevels.add(AccessLevel.Create);
				}

			} else {
				accessLevels.add(AccessLevel.Read);
				accessLevels.add(AccessLevel.Create);
				accessLevels.add(AccessLevel.Modify);
			}

		} else {
			accessLevels.add(AccessLevel.Read);
			accessLevels.add(AccessLevel.Create);
			accessLevels.add(AccessLevel.Modify);
			accessLevels.add(AccessLevel.Delete);
		}

		if(assertDriveItemAccess(path, drive, AccessLevel.Archive)) {
			accessLevels.add(AccessLevel.Archive);
		}

		if(assertDriveItemAccess(path, drive, AccessLevel.Restore)) {
			accessLevels.add(AccessLevel.Restore);
		}

		return accessLevels;
	}
	
	public static boolean assertDriveItemAccess(String path, Drive drive, AccessLevel level, List<String> roles, String username) {
		// Let the admin do anything.  An admin could change any rule anyway.
		boolean hasAccess = false;//isInGroup(ADMIN.getGroupName(), roles);

		// If it's not an admin, check whether rules apply
		if (!hasAccess) {
			for (DriveSecurityRule rule : drive.getSecurityRules()) {
				if (rule.isApplicableToEvaluate(level, roles, username)) {
					if (rule.passesRule(path)) {

						// if an inclusion rule is passed, then the user will have access,
						// unless a later exclusionary rule overrides this one
						if (!rule.isExclude()) {
							hasAccess = true;
						}
						// else - if an exclusion rule is passed, we don't know
						// that the user does have access.  We just know that
						// he is not forbidden access.
					}
					// If this rule is not passed AND this is an exclude rule,
					// the user does NOT have access
					else if (rule.isExclude()) {
						return false;
					}
					// If this path is an ancestor directory of an inclusion rule
					// that allows access, then this ancestor directory should be readable
					else if (rule.matchesAncestorDirectory(path) && Read.equals(level)) {
						hasAccess = true;
					}
				}	// is applicable
			}	// for
		}	// if not an admin
		return hasAccess;
	}

	public static boolean assertDriveItemAccess(String path, Drive drive, AccessLevel level) {
		// Let the admin do anything.  An admin could change any rule anyway.
		boolean hasAccess = false;

		// If it's not an admin, check whether rules apply
		if (!hasAccess) {
			for (DriveSecurityRule rule : drive.getSecurityRules()) {
				if (rule.isApplicableToEvaluate(level)) {
					if (rule.passesRule(path)) {

						// if an inclusion rule is passed, then the user will have access,
						// unless a later exclusionary rule overrides this one
						if (!rule.isExclude()) {
							hasAccess = true;
						}
						// else - if an exclusion rule is passed, we don't know
						// that the user does have access.  We just know that
						// he is not forbidden access.
					}
					// If this rule is not passed AND this is an exclude rule,
					// the user does NOT have access
					else if (rule.isExclude()) {
						return false;
					}
					// If this path is an ancestor directory of an inclusion rule
					// that allows access, then this ancestor directory should be readable
					else if (rule.matchesAncestorDirectory(path) && Read.equals(level)) {
						hasAccess = true;
					}
				}	// is applicable
			}	// for
		}	// if not an admin
		return hasAccess;
	}

	@OPTIONS
	@Path("list")
	public Response listDrivesOPTIONS() {
		return populateSuccessResponse();
	}

	public List<Drive> getDrives() throws AuthorizationException {
		List<Drive> drives = entityManager.createQuery("from Drive", Drive.class).getResultList();
		return drives;
	}

	List<Drive> getDrivesForAdmin() throws AuthorizationException {
		List<Drive> drives = null;
		UserRole userRole = getUserRole();

		switch (userRole) {
			case ADMIN:
				drives = getDrives();
				break;
			case DRIVE_ADMIN:
				String userName = getCurrentUsername();
				drives = DriveUser.getDrivesByUserName(userName, getEntityManager());
				break;
			default:
				drives = new ArrayList<>();
				break;
		}
		return drives;
	}


	public static Drive getDrive(int driveId) throws IOException, AuthorizationException {
		List<Drive> drives = Drive.getDrivesByDriveId(driveId, getEntityManager());

		if (!drives.isEmpty()) {
			return drives.get(0);
		} else {
			throw new IOException("Drive with the following id does not exist: " + driveId);
		}
	}

	@GET
	@Path("list")
	public Response listDrives(@Context HttpServletRequest request) {
		Response response = null;

		try {
			logRequest(request, null);
			List<Drive> drives = null;

			boolean isAdminTab = "true".equals(request.getParameter("isAdminTab"));

			if (!isAdminTab) {
				drives = getDrives();
				drives.removeIf(drive -> !drive.hasReadAccess());
			}
			else {
				drives = getDrivesForAdmin();
			}

			for (Drive drive : drives) {
				try {

					if (!isAdminTab) {
						drive.voidMappedClasses();
					}
					else {
						drive.voidMappedClassesAdmin();
					}

					DriveType driveType = drive.getDriveType();
					if (driveType != null) {
						StorageProvider storageProvider = StorageProviderFactory.getProvider(driveType);
						if (storageProvider instanceof CloudStorageProvider) {
							drive.setStorageClasses(((CloudStorageProvider) storageProvider).getStorageClasses());
						}
						if (storageProvider instanceof RestorableCloudStorageProvider) {
							drive.setRequiresDaysToExpire(((RestorableCloudStorageProvider) storageProvider).requiresDaysToExpire());
						}
						drive.populateProviderProperties(storageProvider.getProperties());
					}

				} catch (ExplorerException e) {
					logger.log(Level.WARNING, e.getMessage(), e);
				}
			}
			
			drives.sort(Comparator.comparing(Drive::getDisplayName));
			response = populateSuccessResponse(drives);
		} catch (AuthorizationException e) {
			String msg = e.getMessage();
			String stackTrace = ExceptionUtils.getStackTrace(e);
			logger.log(Level.SEVERE, msg, e);
			ActionAudit action = auditAction("listDrives", null, 0,
					UNAUTHORIZED, null, 0, msg, stackTrace);
			response = populateUnauthorizedResponse(action);
		} catch (Exception e) {
			String msg = e.getMessage();
			String stackTrace = ExceptionUtils.getStackTrace(e);
			logger.log(Level.WARNING, msg, e);
			ActionAudit action = auditAction("listDrives", null,
					0, ERROR, null, 0, msg, stackTrace);
			response = populateResponseOnException(action);
		}
		return response;
	}

	protected List<FolderMemoryUsageDTO> buildFolderUsageDTOs(int driveId, List<DriveItem> driveItems) {
		EntityManager mgr = getEntityManager();
		List<FolderMemoryUsageDTO> usageDTOs = new ArrayList<>();

		// Get usage for each subfolder and add a corresponding DTO
		for (DriveItem item : driveItems) {
			String childPath = item.getPath();
			
			if (childPath.endsWith(GUI_SEP)) { // It's a folder, not a file
				Long numBytes = MemoryUsage.getFolderUsage(driveId, childPath, mgr);

				FolderMemoryUsageDTO dto = new FolderMemoryUsageDTO();
//					dto.setCreated(usage.getCreated());
				dto.setPath(item.getPath());
				dto.setBytes(numBytes);
				usageDTOs.add(dto);
			}
		}
		return usageDTOs;
	}

	@OPTIONS
	@Path("delete")
	public Response deleteOPTIONS() {
		return populateSuccessResponse();
	}

	@POST
	@Path("delete")
	public Response delete(DriveItemListSpec spec, @Context HttpServletRequest request, @Context HttpServletResponse resp) {

		int driveId = spec.getDriveId();
		Drive drive = null;
		StorageProvider provider;

		//gets the provider and drive for the drive items
		try {

			drive = getDrive(driveId);
			DriveType driveType = drive.getDriveType();
			provider = StorageProviderFactory.getProvider(driveType, request);
		} catch (Exception e) {
			boolean isUnauthorized = e instanceof AuthorizationException;
			logger.log(Level.WARNING, e.getMessage(), e);

			List<ActionAudit> audits = new ArrayList<>();
			for (DriveItem item : spec.getDriveItems()) {
				String path = item.getPath();

				// Provide audit data for each delete attempt
				ActionAudit audit = this.auditAction("delete", path, driveId, isUnauthorized ? UNAUTHORIZED : ERROR);
				audits.add(audit);
				if (isUnauthorized) {
					recordUnauthorized(audit, e);
				} else {
					recordException(audit, e);
				}
			}
			return isUnauthorized ? populateUnauthorizedResponse(audits) :
					populateResponseOnException(audits);
		}

		List<ActionAudit> audits = new ArrayList<>();

		for (DriveItem item : spec.getDriveItems()) {
			String path = item.getPath();

			// Provide audit data for each delete attempt
			ActionAudit audit = this.auditAction("delete", path, driveId, PENDING);
			audits.add(audit);

			try {
				if (assertDriveItemAccess(path, drive, AccessLevel.Delete)) {
					provider.delete(drive, path);
					recordSuccess(audit);
				} else {
					recordUnauthorized(audit);
				}
			} catch (Exception e) {
				logger.log(Level.WARNING, e.getMessage(), e);
				recordException(audit, e);
			}
		}

		return populateSuccessResponse(audits);
	}

	@POST
	@Path("exists")
	public Response exists(FileLocationSpec spec, @Context HttpServletRequest request, @Context HttpServletResponse resp) {
		Response response = null;

		ActionAudit action = auditAction("exists", spec.getPath(), spec.getDriveId(), PENDING,
				null, 0, null, null);

		try {
			Drive drive = getDrive(spec.getDriveId());
			StorageProvider provider = StorageProviderFactory.getProvider(drive.getDriveType(), request);

			boolean exists = provider.exists(drive, spec.getPath());
			recordSuccess(action);
			response = populateSuccessResponse(exists);
		} catch (AuthorizationException e) {
			logger.log(Level.SEVERE, e.getMessage(), e);

			recordUnauthorized(action);
			response = populateUnauthorizedResponse(action);
		} catch (Exception e) {
			logger.log(Level.WARNING, e.getMessage(), e);
			recordException(action, e);
			response = populateResponseOnException(action);
		}
		return response;
	}

	@POST
	@Path("anyExists")
	public Response anyExists(ListFileLocationSpec spec, @Context HttpServletRequest request, @Context HttpServletResponse resp) {

		Response response = null;

		ActionAudit action = auditAction("exists", spec.getPathsAsString(), spec.getDriveId(), PENDING,
				null, 0, null, null);
		try {

			Drive drive = getDrive(spec.getDriveId());
			StorageProvider provider = StorageProviderFactory.getProvider(drive.getDriveType(), request);

			
			HashMap<String, List<DriveItem>> searchMatchers = new HashMap<String, List<DriveItem>>();
			
			
			boolean exists = false;
			for (String path : spec.getPaths()) {
				
				if(searchMatchers.containsKey(PathProcessor.getParentFolderPath(path)) == false) {

					DriveQuery dq = new DriveQuery();
					dq.setDriveId(drive.getDriveId());
					dq.setRecursive(false);
					dq.setStartPath(PathProcessor.getParentFolderPath(path));
					
					searchMatchers.put(PathProcessor.getParentFolderPath(path), provider.find(drive, dq));
				}
				
				String fileName = PathProcessor.getFileName(path);
				
				exists =
				searchMatchers.get(PathProcessor.getParentFolderPath(path)).stream().anyMatch(driveItem -> driveItem.getFileName().compareToIgnoreCase(fileName) == 0);
				
				if (exists) {
					break;
				}
			}

			response = populateSuccessResponse(exists);
		} catch (AuthorizationException e) {
			logger.log(Level.SEVERE, e.getMessage(), e);
			recordUnauthorized(action);
			response = populateUnauthorizedResponse(action);
		} catch (Exception e) {
			logger.log(Level.WARNING, e.getMessage(), e);
			recordException(action, e);
			response = populateResponseOnException(action);
		}
		return response;
	}

	@POST
	@Path("rename")
	public Response rename(TransferSpec spec, @Context HttpServletRequest request, @Context HttpServletResponse resp) {
		Response response = null;

		String sourcePath = spec.getSourcePath();
		String destPath = spec.getDestPath();
		ActionAudit action = auditAction("rename", sourcePath, spec.getSourceDriveId(), PENDING,
				destPath, spec.getDestDriveId(), null, null);

		try {
			Drive sourceDrive = getDrive(spec.getSourceDriveId());
			Drive destDrive = getDrive(spec.getDestDriveId());

			if (sourceDrive == null) {
				throw new MissingArgumentException("No source drive provided");
			}
			if (sourcePath == null) {
				throw new MissingArgumentException("No source path provided");
			}
			if (destPath == null) {
				throw new MissingArgumentException("No destination path provided");
			}
			if (destDrive == null) {
				throw new MissingArgumentException("No destination drive provided");
			}

			if (!assertDriveItemAccess(sourcePath, sourceDrive, AccessLevel.Read)
					|| !assertDriveItemAccess(destPath, destDrive, AccessLevel.Create)) {
				throw new AuthorizationException(
						"User does not have sufficient permissions to rename " +
								sourcePath + " on " + sourceDrive + " to " +
								destPath + " on " + destDrive);
			}

			DriveType sourceDriveType = sourceDrive.getDriveType();

			// Handle case where the source and destination are not the same
			if (spec.getSourceDriveId() != spec.getDestDriveId()) {
				throw new ExplorerException(sourcePath + " cannot be renamed across drives", true);
			}

			StorageProvider provider = StorageProviderFactory.getProvider(sourceDriveType, request);

			if (provider.exists(destDrive, destPath)) {
				throw new ExplorerException("Error occurred during rename; path already exists: " + destPath, true);
			}

			provider.rename(sourceDrive, sourcePath, destPath);
			response = populateSuccessResponse();

			recordSuccess(action);
		} catch (AuthorizationException e) {
			logger.log(Level.SEVERE, e.getMessage(), e);
			recordUnauthorized(action, e);
			response = populateUnauthorizedResponse(action);
		} catch (Exception e) {
			logger.log(Level.WARNING, e.getMessage(), e);
			recordException(action, e);
			response = populateResponseOnException(action);
		}
		return response;
	}

	@OPTIONS
	@Path("query")
	public Response queryDriveOPTIONS() {
		return populateSuccessResponse();
	}

	@POST
	@Path("query")
	public Response queryDrive(final DriveQuery query, @Context HttpServletRequest request,
							   @Context HttpServletResponse resp) {

		Response response = null;
		try {
			logRequest(request, query);

			if (query == null) {
				throw new ExplorerException("queryDrive - null query");
			}
			int driveId = query.getDriveId();
			Drive currentDrive = getDrive(driveId);

			DriveType driveType = currentDrive.getDriveType();
			StorageProvider provider = StorageProviderFactory.getProvider(driveType, request);
			List<DriveItem> driveItems = findDriveItems(query, currentDrive, provider);
			response = populateSuccessResponse(driveItems);
		} catch (AuthorizationException e) {
			String msg = e.getMessage();
			String stackTrace = ExceptionUtils.getStackTrace(e);
			logger.log(Level.SEVERE, e.getMessage(), e);
			ActionAudit action = auditAction("query", query.getStartPath(),
					query.getDriveId(), UNAUTHORIZED, null, 0, msg, stackTrace);
			response = populateUnauthorizedResponse(action);
		} catch (Exception e) {
			String msg = "search pattern: " + query.getSearchPattern();
			String stackTrace = ExceptionUtils.getStackTrace(e);
			logger.log(Level.WARNING, e.getMessage(), e);
			ActionAudit action = auditAction("query", query.getStartPath(),
					query.getDriveId(), ERROR, null, 0, msg, stackTrace);
			response = populateResponseOnException(action);
		}
		return response;
	}

	protected List<DriveItem> findDriveItems(final DriveQuery query, Drive drive, StorageProvider provider)
			throws ExplorerException {
		long preFind = System.currentTimeMillis();
		List<DriveItem> driveItems = provider.find(drive, query);
		logger.info("findDriveItems.find took " +
				(System.currentTimeMillis() - preFind ) + " ms.");

		long preCollect = System.currentTimeMillis();
		driveItems = driveItems.stream()
				.filter(item -> assertDriveItemAccess(item.getPath(), drive, Read))
				.collect(Collectors.toList());
		logger.info("findDriveItems.collect took " +
				(System.currentTimeMillis() - preCollect ) + " ms.");

		for (DriveItem item : driveItems) {
			item.setAccessLevels(getDriveItemAccess(item.getPath(), drive));
		}
		logger.info("findDriveItems took " +
				(System.currentTimeMillis() - preFind ) + " ms.");
		return driveItems;
	}

	@OPTIONS
	@Path("mkdir")
	public Response mkdirOPTIONS() {
		return populateSuccessResponse();
	}

	@POST
	@Path("mkdir")
	public Response mkdir(FileLocationSpec spec, @Context HttpServletRequest request,
						  @Context HttpServletResponse resp) {
		Response response = null;

		ActionAudit action = this.auditAction("mkdir", spec.getPath(), spec.getDriveId(), PENDING);

		try {
			logRequest(request, spec);
			Drive currentDrive = getDrive(spec.getDriveId());

			if (currentDrive != null) {

				try {
					if (assertDriveItemAccess(spec.getPath(), currentDrive, AccessLevel.Create)) {
						DriveType driveType = currentDrive.getDriveType();
						StorageProvider provider = StorageProviderFactory.getProvider(driveType, request);
						provider.mkdir(currentDrive, spec.getPath());
						response = populateSuccessResponse();
						recordSuccess(action);
					} else {
						recordUnauthorized(action);
						response = populateUnauthorizedResponse(action);
					}
				} catch (Exception e) {
					recordException(action, e);
					response = populateResponseOnException(action);
				}
			}
		} catch (AuthorizationException e) {
			logger.log(Level.SEVERE, e.getMessage(), e);
			recordUnauthorized(action);
			response = populateUnauthorizedResponse(action);
		} catch (Exception e) {
			logger.log(Level.WARNING, e.getMessage(), e);
			recordException(action, e);
			response = populateResponseOnException(action);
		}
		return response;
	}

	@OPTIONS
	@Path("addExternalTableReference")
	public Response addExternalTableReferenceOPTIONS() {
		return populateSuccessResponse();
	}

	/**
	 * Enable a Hive metastore to reference a table stored
	 * on a cloud storage provider.  (a.k.a. "add to BDV)
	 * This is only supported for azure, gcp and aws.
	 */
	@POST
	@Path("addExternalTableReference")
	public Response addExternalTableReference(final ExternalTableSpec spec,
			@Context HttpServletRequest request,
			@Context HttpServletResponse resp) {
		Response response = null;

		int sourceDriveId = spec.getDriveId();
		String path = spec.getPath();
		String dbName = spec.getDatabaseName();
		String table = spec.getTableName();
		// For auditing purposes, show the destination as the
		// db name combined with the table name
		String auditDest = dbName + ":" + table;

		ActionAudit audit = this.auditAction("addExternalTableReference",
				path, sourceDriveId, PENDING,
				auditDest, Drive.UNKNOWN_DRIVE_ID, null, null);

		try {
			logRequest(request, spec);
			Drive drive = getDrive(sourceDriveId);
			if (drive == null) {
				return populateResponseOnMissingInput("No source drive provided.");
			}

			if (dbName == null) {
				return populateResponseOnMissingInput("No database name provided.");
			}

			if (table == null) {
				return populateResponseOnMissingInput("No table name provided.");
			}

			DriveType driveType = drive.getDriveType();
			StorageProvider provider =
					StorageProviderFactory.getProvider(driveType, request);

			if (assertDriveItemAccess(path, drive, AccessLevel.Read)) {

				DownloadSpec specDL = new DownloadSpec();
				specDL.setDriveId(drive.getDriveId());
				specDL.setPath(path);

				BasicFile tempFile = downloadToTempBasicFile(specDL, request, path, false, null,
						drive);
				try {
					String extension = PathProcessor.getExtension(path).toLowerCase();
					Table tableDef = null;
					HiveTableMaker hive = new HiveTableMaker();

					String location = provider.getHiveLocationPath(drive, path);

					switch (extension) {
						case "avro":
						{
							AVROFileProcessor avro = new AVROFileProcessor();
							Schema schema = avro.getSchema(tempFile.getCanonicalPath());

							// Make sure there is an AVSC file (schema), in the
							// directory above the AVRO file
							String baseName = FilenameUtils.getBaseName(path);
							String folder = PathProcessor.getParentFolder(PathProcessor.removeLastSlash(path));
							String folderAbove = PathProcessor.getParentFolder(PathProcessor.removeLastSlash(folder));
							String avscPath = PathProcessor.addLastSlash(folderAbove) + baseName + ".avsc";
							BasicFile tempAVSCFile = null;

							try {
								// If there isn't already an AVSC file in the
								// directory above, create one
								if (!provider.exists(drive, avscPath)) {
									tempAVSCFile = buildTempFile("DS_AVSC");
									String json = schema.toString(true); // true -> pretty-print
									FileUtils.writeStringToFile(tempAVSCFile, json, StandardCharsets.UTF_8);
									provider.upload(drive, avscPath, tempAVSCFile);
								}
								tableDef = hive.createExternalAVROTable(path, dbName, table, location, schema, ',');
							} finally {
								if (tempAVSCFile != null) {
									tempAVSCFile.delete();
								}
							}
						}
						break;
						case "csv":
						{
							CSVFileProcessor processor = new CSVFileProcessor();
							ResultSetMetaData schema = processor.getSchema(tempFile.getAbsolutePath());
							tableDef = hive.createExternalCSVTable(dbName, table, location, schema, ',');
						}
						break;
						case "parquet":
						{
							ParquetFileProcessor processor = new ParquetFileProcessor();
							MessageType schema = processor.getSchema(tempFile.getAbsolutePath());
							tableDef = hive.createExternalParquetTable(dbName, table, location, schema);
						}
						break;

						default:
						{
							String msg = "Can't generate schema from " + extension + " file.";
							logger.warning(msg);
							throw new ExplorerException(msg);
						}
					}

					try (HiveConnector connector = new HiveConnector(drive)) {
						connector.createTable(tableDef);
					}

					recordSuccess(audit, 0L);
					response = populateSuccessResponse();

				} finally {
					tempFile.delete();
				}
			} else {
				recordUnauthorized(audit);
				response = populateUnauthorizedResponse(audit);
			}
		} catch (AuthorizationException e) {
			logger.log(Level.SEVERE, e.getMessage(), e);
			recordUnauthorized(audit);
			response = populateUnauthorizedResponse(audit);
		} catch (Exception e) {
			String msg = e.getMessage();
			String stackTrace = ExceptionUtils.getStackTrace(e);
			logger.log(Level.WARNING, msg, e);
			audit.setMessage(msg);
			audit.setStackTrace(stackTrace);
			recordException(audit, e);
			response = populateResponseOnException(audit);
		}
		return response;
	}


	@OPTIONS
	@Path("transferBatch")
	public Response transferBatchOPTIONS() {
		return populateSuccessResponse();
	}
	
	private ExecutorService executorService = java.util.concurrent.Executors.newSingleThreadExecutor();

	@POST
	@Path("transferBatch")
	public Response transferBatch(final List<TransferSpec> specs, @Context HttpServletRequest request,
							 @Context HttpServletResponse resp) throws PolicyContextException, InterruptedException, ExecutionException {

		List<String> roles = getUserRoles();

        TransferTask task = new TransferTask(specs, request, entityManager, utx, request.getAttribute("spnegoprin") == null ? null : (SpnegoPrincipal)request.getAttribute("spnegoprin"), roles, getCurrentUsername(), getIpAddress());
        //Future<Boolean> future = 
        Future<Boolean> ret = executorService.submit(task);
        executorService.shutdown();
        executorService.awaitTermination(100000, TimeUnit.DAYS);
        if(ret.get() == true) {
            return populateSuccessResponse();
        } else {
        	return populateResponseOnException(new ActionAudit());
        }
	
	}

	private List<String> getUserRoles() throws PolicyContextException {
		
		ArrayList<String> ret = new ArrayList<String>();
		
		Roles roles = org.wildfly.security.auth.server.SecurityDomain.getCurrent().getCurrentSecurityIdentity().getRoles();
		//Subject subject = (Subject) PolicyContext.getContext("javax.security.auth.Subject.container");
		for (String role : roles) {
			ret.add(role);
		}
		return ret;
	}

	@OPTIONS
	@Path("transfer")
	public Response transferOPTIONS() {
		return populateSuccessResponse();
	}

	@POST
	@Path("transfer")
	public Response transfer(final TransferSpec spec, @Context HttpServletRequest request,
							 @Context HttpServletResponse resp) throws PolicyContextException, InterruptedException, ExecutionException {

		List<String> roles = getUserRoles();

        TransferTask task = new TransferTask(Arrays.asList(spec), request, entityManager, utx, request.getAttribute("spnegoprin") == null ? null : (SpnegoPrincipal)request.getAttribute("spnegoprin"), roles, getCurrentUsername(), getIpAddress());
        //Future<Boolean> future = 
        Future<Boolean> ret = executorService.submit(task);
        executorService.shutdown();
        executorService.awaitTermination(100000, TimeUnit.DAYS);
        if(ret.get() == true) {
            return populateSuccessResponse();
        } else {
        	return populateResponseOnException(new ActionAudit());
        }
	}

	String getRelativePath(StorageProvider provider, String dirIn, String fileIn) {
		String dir = provider.normalizePath(dirIn);
		String file = provider.normalizePath(fileIn);
		String relative = file.startsWith(dir) ? file.substring(dir.length()) : file;
		// TODO handle cases where the provider allows slashes or backslashes as part of
		// the name
		relative = (relative.startsWith("/") || relative.startsWith("\\"))
				? relative.substring(1) : relative;
		return relative;
	}

	public static void transferOneFile(DriveItem driveItem, final TransferSpec spec, Drive sourceDrive, Drive destDrive, ActionAudit action,
						 SpnegoPrincipal principal, List<String> roles, ActionAudit batchAction, String username, String _resolution) throws Exception {

		if (driveItem.isRestoreRequired()) {
			throw new ArchiveException("Cannot transfer file because it is archived");
		}

		StorageProvider sourceProvider =
				StorageProviderFactory.getProvider(sourceDrive.getDriveType(), principal);
		StorageProvider destProvider =
				StorageProviderFactory.getProvider(destDrive.getDriveType(), principal);
		String sourcePath = spec.getSourcePath();
		String destPath = spec.getDestPath();
		
		if (driveItem.isDirectory()) {
			if(destProvider.exists(destDrive, destPath) == false) {
				destProvider.mkdir(destDrive, destPath);
			}
		} else {
			boolean exi = destProvider.exists(destDrive, destPath);
			
			if(exi == true) {
				if(_resolution.compareTo("cancel") == 0) {
						throw new DriveItemExistsException("Unable to transfer " +
							sourcePath + " on " + sourceDrive + " to " +
							destPath + " on " + destDrive + " because it already exists.");
				} else if (_resolution.compareTo("overwrite") == 0) {
					destProvider.delete(destDrive, destPath);
				} else if(_resolution.compareTo("skip") == 0) {
					action.setAction("skip");
					return;
				}
			}
			
		}

		if (driveItem.isEmpty()) {//is empty AND is a file
			BasicFile tempFile = buildTempFile("DS_transferOneFile");

			destProvider.upload(destDrive, destPath, tempFile);
			deleteTempFile(tempFile);

			if (spec.isRemoveSource()) {
				sourceProvider.delete(sourceDrive, sourcePath);
			}
		}
		// If the source and destination drives are the same,
		// then we are able to do the transfer efficiently
		// using just that server
		else if (sourceDrive.equals(destDrive)) {

			if (spec.isRemoveSource()) { // move
				sourceProvider.rename(sourceDrive, sourcePath, destPath);
			} else { // copy
				sourceProvider.copy(sourceDrive, sourcePath, destPath);
			}

		}
		// The drives are different.  Transfer using chunks.
		else {
			transferUsingChunks(driveItem, spec, sourceDrive, destDrive, action, principal, roles, batchAction, username);
		}
	}

	/**
	 * Transfer a single file (e.g. copy) from one location to another,
	 * and record the size of the transfer in the audit object.
	 * @param driveItem 
	 */
	static void transferUsingChunks(DriveItem driveItem, final TransferSpec spec,
									 Drive sourceDrive, Drive destDrive, ActionAudit audit,
							 SpnegoPrincipal principal, List<String> roles, ActionAudit batchAction, String username) throws Exception {
		DriveType sourceDriveType = sourceDrive.getDriveType();
		String sourcePath = spec.getSourcePath();
		StorageProvider sourceProvider = StorageProviderFactory.getProvider(sourceDriveType, principal);

		// To be able to perform a "move", the user must have
		// permission to remove the source file.
		if (spec.isRemoveSource() && !assertDriveItemAccess(sourcePath, sourceDrive, AccessLevel.Delete, roles, username)) {
			throw new AuthorizationException(
					"User does not have permissions to delete " + sourcePath + " on " + sourceDrive);
		}

		DriveType destDriveType = destDrive.getDriveType();
		StorageProvider destProvider =
				StorageProviderFactory.getProvider(destDriveType, principal);
		String specDestPath = spec.getDestPath();
		String destPath = destProvider.normalizePath(specDestPath);
		
		int basePercentBatch = batchAction.getPercentComplete() == null ? 0 : batchAction.getPercentComplete();
		double totalPercent = (1d / batchAction.getTotalBytes()) * 100;

		if (destProvider.exists(destDrive, destPath)) {
			throw new DriveItemExistsException("Transfer cancelled - " + destPath + " already exists.");
		}

		// Don't try to upload a directory.
		// Directories may be created as part of the file upload
		if (!sourceProvider.isDirectory(sourceDrive, sourcePath)) {
			long fileSize = driveItem.getFileSize();

			// For files over 100 GB, we need to increase the chunk size.
			int partSize = getChunkSize(fileSize);

			String uploadId = destProvider.uploadPartStart(destDrive, destPath);

			try {
				long startByte = 0;
				int partNumber = 1;

				sourceProvider.downloadPartStart(sourceDrive, sourcePath);
				byte[] data = sourceProvider.downloadBytes(sourceDrive, sourcePath, startByte, partSize);

				if (data.length > 0) {
					destProvider.uploadPart(destDrive, destPath, data, partNumber);
				}

				while (data.length == partSize) {
					startByte += partSize;

					data = downloadBytes(sourceDrive, audit, sourcePath, sourceProvider, partSize, startByte);

					int retryCount = 20;
					Exception exception = null;
					boolean complete = false;

					// Upload one part
					partNumber++;
					while (retryCount > 0 && !complete) {
						try {
							retryCount--;
							destProvider.uploadPart(destDrive, destPath, data, partNumber);
							complete = true;
						} catch (Exception e) {
							exception = e;
							handleRetryException(audit, e, retryCount);
							Thread.sleep(1000);
						}
					}

					batchAction = BaseService.entityManager.find(ActionAudit.class, batchAction.getActionAuditId());

					if(batchAction.getStatus().compareTo("A") == 0) {
						batchAction.setStatus("C");
						BaseService.updateAction(batchAction);
						Exception ex = new Exception("Cancelled by user");
						abortTransfer(sourceProvider, sourceDrive, sourcePath, destProvider, destDrive, destPath, uploadId, ex);
						throw ex;
					}
					
					int percent = (int) Math.round(((double)startByte / fileSize) * totalPercent) + basePercentBatch;
					batchAction.setPercentComplete(percent);
					BaseService.updateAction(batchAction);

					// Failed to upload a part after multiple attempts,
					// so return a failure response
					if (!complete) {
						throw exception;
					}
				}

				destProvider.uploadPartComplete(destDrive, destPath, uploadId);
				sourceProvider.downloadComplete(sourceDrive, sourcePath);

			} catch (Exception ex) {    // abort the upload and download
				abortTransfer(sourceProvider, sourceDrive, sourcePath, destProvider, destDrive, destPath, uploadId, ex);
				throw ex; // rethrow original exception
			}

			if (spec.isRemoveSource()) {
				sourceProvider.delete(sourceDrive, sourcePath);
			}
		}
	}

	static int getChunkSize(long fileSize) {
		long timesLarger =
				// This first part returns 1 less than the number of
				// multiples, because int division truncates
				((fileSize - 1) / MAX_SIZE_TRANSFERRABLE_USING_DEFAULTS)
				// add one to get the right number
				+ 1;
		long partSizeL = timesLarger * DEFAULT_UPLOAD_PART_SIZE;
		
		// TODO Our existing interfaces expect an int for chunk size,
		// but the cloud storage providers can use larger chunks.
		// This is good enough to handle ~ 20 TB files
		int partSize =
				(partSizeL > Integer.MAX_VALUE)
				? Integer.MAX_VALUE // 2 GB
				: (int)partSizeL;
		return partSize;
	}

	static void abortTransfer(
			StorageProvider sourceProvider, Drive sourceDrive, String sourcePath,
			StorageProvider destProvider, Drive destDrive, String destPath,
			String uploadId, Exception ex)
	{
		logger.info("Aborting download and upload after receiving" + ex);
		
		try {
			destProvider.uploadPartAbort(destDrive, destPath, uploadId);
		}
		catch (Exception e1) {
			// Log the problem with uploadPartAbort, but the calling
			// code will rethrow the original, root cause exception
			logger.log(Level.WARNING,
					"after exception, uploadPartAbort failed: " + e1.getMessage(), e1);
		}
		
		try {	// there is no abort method for downloads, so call downloadComplete
			sourceProvider.downloadComplete(sourceDrive, sourcePath);
		}
		catch (Exception e1) {
			// Log the problem with downloadComplete, but the calling
			// code will rethrow the original, root cause exception
			logger.log(Level.WARNING,
					"after exception, downloadComplete failed: " + e1.getMessage(), e1);
		}
	}

	public BasicFile preview(StorageProvider provider, Drive drive, long fileSize, DownloadSpec spec, ActionAudit action) throws Exception {
		String path = spec.getPath();
		int numTop = spec.getTopLines();
		int numBottom = spec.getBottomLines();
		final int downloadPartSize = 5000;
		final long maxSearchBytes = 500_000L;
		long byteIndex = 0L;
		long endByte = fileSize - downloadPartSize;

		try {
			provider.downloadPartStart(drive, path);
			BasicFile tempFile = buildTempFile("DS_preview");

			byte[] allStartBytes = getTopLines(provider, drive, path, numTop, downloadPartSize, byteIndex, endByte, maxSearchBytes,
					action);
			byteIndex = allStartBytes.length;

			long totalSearchBytes = allStartBytes.length;
			int bottomLinesCount = 0;
			byte[] allEndBytes = new byte[0];
			while ((bottomLinesCount <= numBottom) && (byteIndex < endByte)) {
				byte[] endBytes = downloadBytes(drive, action, path, provider, downloadPartSize, endByte);

				int index = 0;
				for (index = endBytes.length - 1; index > 0; index--) {
					byte b = endBytes[index];

					if (isNewLine(b)) {
						bottomLinesCount++;

						if (bottomLinesCount > numBottom) {
							break;
						}
						if (((index - 1) > 0) && isNewLine(endBytes[index - 1])) {
							index--;
						}
					}
				}
				byte[] bytes = Arrays.copyOfRange(endBytes, index, endBytes.length - 1);

				allEndBytes = Bytes.concat(bytes, allEndBytes);

				endByte -= downloadPartSize;
				totalSearchBytes += downloadPartSize;

				if (totalSearchBytes > maxSearchBytes) {
					throw new NotFoundException("Search took too long to find the last \"" + numBottom + "\" lines");
				}
			}

			//if the end is found, the file is too small to be previewed, so download it
			if (byteIndex > endByte) {
				tempFile = provider.download(drive, path);
			} else {
				FileUtils.writeByteArrayToFile(tempFile, allStartBytes, true);
				FileUtils.writeByteArrayToFile(tempFile, allEndBytes, true);
			}

			return tempFile;
		} finally {
			provider.downloadComplete(drive, path);
		}
	}

	/**
	 * Get the topmost lines of a file
	 * @param numTop how many lines to get from the top of the file
	 * @return the number of bytes read
	 */
	byte[] getTopLines(StorageProvider provider, Drive drive, String path, int numTop, final int downloadPartSize,
					   long byteIndex, long endByte, final long maxSearchBytes, ActionAudit action) throws Exception {
		int topLinesCount = 0;

		byte[] allBytes = new byte[0];
		do {
			byte[] startBytes = downloadBytes(drive, action, path, provider, downloadPartSize, byteIndex);

			int index;
			for (index = 0; index < startBytes.length; index++) {
				byte b = startBytes[index];

				if (isNewLine(b)) {
					topLinesCount++;

					if (topLinesCount >= numTop) {
						break;
					}
					if (((index + 1) < startBytes.length) && isNewLine(startBytes[index + 1])) {
						index++;
					}
				}
			}

			byte[] bytes = Arrays.copyOfRange(startBytes, 0, index);
			allBytes = Bytes.concat(allBytes, bytes);
			byteIndex += downloadPartSize;

			if (byteIndex > maxSearchBytes) {
				throw new NotFoundException("Search took too long to find the first \"" + numTop + "\" lines");
			}
		} while ((topLinesCount < numTop) && (byteIndex < endByte));
		return allBytes;
	}

	private static byte[] downloadBytes(Drive sourceDrive, ActionAudit audit, String sourcePath, StorageProvider sourceProvider, int downloadPartSize, long startByte) throws Exception {

		int retryCount = 20;
		Exception exception = null;

		// Download one part
		while(retryCount > 0) {
			try {
				retryCount--;
				return sourceProvider.downloadBytes(sourceDrive, sourcePath, startByte, downloadPartSize);
			} catch (Exception e) {
				exception = e;
				handleRetryException(audit, e, retryCount);
				Thread.sleep(1000);
			}
		}

		// Failed to download a part after multiple attempts,
		// so throw the exception
		throw exception;
	}

	static void handleRetryException(ActionAudit audit, Exception e, int retryCount) {
		String message = "RetryCount " + retryCount + ": " + e.getMessage();

		// record failure in detail, with stack trace
		if (retryCount == 0) {
			logger.log(Level.WARNING, message, e);
			recordException(audit, e);
		}
		// Just show the message (avoid cluttering the log
		// with 20 stack traces
		else {
			logger.log(Level.WARNING, message);
		}
	}

	@OPTIONS
	@Path("upload")
	public Response uploadFileOPTIONS() {
		return populateSuccessResponse();
	}

	@POST
	@Path("uploadEmptyFile")
	@Consumes(MediaType.APPLICATION_JSON)
	public Response uploadEmptyFile(FileLocationSpec input, @Context HttpServletRequest request,
									@Context HttpServletResponse resp) throws IOException {
		Response response = null;

		logRequest(request, input);

		int driveId = input.getDriveId();
		String path = input.getPath();

		ActionAudit action = this.auditAction("upload", path, driveId, PENDING);

		try {

			Drive currentDrive = getDrive(driveId);

			if (assertDriveItemAccess(path, currentDrive, AccessLevel.Create) == true) {

				// convert the uploaded file to inputstream

				File tempFile = buildTempFile("DS_uploadEmptyFile");

				if (currentDrive != null) {

					try {
						StorageProvider provider = StorageProviderFactory
								.getProvider(currentDrive.getDriveType(), request);

						if (provider.exists(currentDrive, path)) {
							throw new FileAlreadyExistsException(
									"Error occurred during upload; path already exists: " + path);
						}

						provider.upload(currentDrive, path, tempFile);
						deleteTempFile(tempFile);
						recordSuccess(action, 0L);
						return populateSuccessResponse();
					} catch (Exception e) {
						deleteTempFile(tempFile);
						recordException(action, e);
						return populateResponseOnException(action);
					}
				}
			} else {
				recordUnauthorized(action);
				return this.populateUnauthorizedResponse(action);
			}

		} catch (AuthorizationException e) {
			logger.log(Level.SEVERE, e.getMessage(), e);
			recordUnauthorized(action);
			response = populateUnauthorizedResponse(action);
		} catch (Exception e) {
			recordException(action, e);
			return populateResponseOnException(action);
		}
		return response;
	}

	@OPTIONS
	@Path("upload/chunk")
	public Response uploadChunkOPTIONS() {
		return populateSuccessResponse();
	}

	@POST
	@Path("upload/chunk")
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	public Response uploadChunk(MultipartFormDataInput input,
			@Context HttpServletRequest request,
			@Context HttpServletResponse resp) throws IOException {
		Response response = null;
		try {
			Map<String, List<InputPart>> uploadForm = input.getFormDataMap();
			int driveId = Integer.parseInt(uploadForm.get("driveId").get(0).getBodyAsString());
			String path = uploadForm.get("path").get(0).getBodyAsString();

			ChunkMetadata meta = getChunkMetadata(uploadForm);
			String fileName = meta.getFileName();
			String uploadPath = addLastSlash(path) + fileName;
			String fileUid = meta.getFileUid();
			String tmpdir = System.getProperty("java.io.tmpdir");
			String tempFileName = "DS_uploadChunk_" + fileUid;
			BasicFile file = new BasicFile(PathProcessor.addLastSlash(tmpdir) + tempFileName);
			logger.info("Processing request " + request.getRequestURL() +
					".  Uploading from " + tempFileName + " to " + uploadPath);

			ActionAudit action = null;

			try {
				if (meta.isStart()) {
					action = this.auditAction("uploadchunk", fileName, driveId, uploadPath, driveId, PENDING);

					Drive drive = getDrive(driveId);
					if (!assertDriveItemAccess(uploadPath, drive, AccessLevel.Create)) {
						throw new AuthorizationException(
								"User does not have create permissions to upload " + uploadPath +
								" to " + drive);
					}

					file.createNewFile();
				}
				// Get file data to save
				List<InputPart> inputParts = uploadForm.get("files");
				writeToTempFile(inputParts.get(0), file);

				if (meta.isEnd()) {
					action = getActionAudit("uploadchunk", fileName, driveId, uploadPath, driveId, PENDING);

					Drive currentDrive = getDrive(driveId);
					DriveType driveType = currentDrive.getDriveType();
					StorageProvider provider = StorageProviderFactory.getProvider(driveType, request);

					provider.upload(currentDrive, uploadPath, file);

					long byteCount = file.length();
					deleteTempFile(file);
					recordSuccess(action, byteCount);
				}
				response = populateSuccessResponse(new ChunkResult(meta.isEnd(), fileUid));

			} catch (AuthorizationException e) {
				deleteTempFile(file);
				action = (action != null) ? action
						: getActionAudit("uploadchunk", fileName, driveId, uploadPath, driveId, PENDING);
				recordUnauthorized(action, e);
				logger.log(Level.SEVERE, e.getMessage(), e);
				response = populateUnauthorizedResponse(action);
			} catch (Exception e) {
				deleteTempFile(file);
				action = (action != null) ? action
						: getActionAudit("uploadchunk", fileName, driveId, uploadPath, driveId, PENDING);

				recordException(action, e);
				response = populateResponseOnException(action);
			}
		} finally {
			// This is necessary due to a temp file resource leak in RestEasy
			// see https://issues.redhat.com/browse/RESTEASY-681?page=com.atlassian.jira.plugin.system.issuetabpanels%3Acomment-tabpanel
			input.close();
		}
		return response;
	}

	BasicFile writeToTempFile(InputPart inputPart, BasicFile tempFile) throws IOException {
		InputStream inputStream = new ThrottledInputStream(inputPart.getBody(InputStream.class, null));

		try (OutputStream writer = new BufferedOutputStream(new FileOutputStream(tempFile, true))) {
			int read = -1;

			while ((read = inputStream.read()) != -1) {
				writer.write(read);
			}
			writer.flush();
		}
		return tempFile;
	}

	ChunkMetadata getChunkMetadata(Map<String, List<InputPart>> uploadForm)
			throws IOException {
		String metadata = uploadForm.get("metadata").get(0).getBodyAsString();
		ObjectMapper mapper = new ObjectMapper();
		ChunkMetadata meta = mapper.readValue(metadata, ChunkMetadata.class);
		return meta;
	}

	@OPTIONS
	@Path("download")
	public Response downloadOPTIONS() {
		return populateSuccessResponse();
	}

	private BasicFile downloadToTempBasicFile(final DownloadSpec spec, HttpServletRequest request,
											  String path, boolean isPreview, ActionAudit action, Drive currentDrive)
			throws Exception {
		BasicFile ret;

		StorageProvider provider = StorageProviderFactory.getProvider(currentDrive.getDriveType(), request);

		DriveItem driveItem = provider.getDriveItem(currentDrive, path);

		if (driveItem != null) {
			long fileSize = driveItem.getFileSize();

			if (isPreview) {
				ret = preview(provider, currentDrive, fileSize, spec, action);
			}
			else if (FileUtil.spaceExists(fileSize)) { // normal download case
				ret = provider.download(currentDrive, path);
			}
			else { // shortage of (temp) disk space for download
				throw new ExplorerException("Insufficient disk space to download: " + path);
			}
		}
		else {
			throw new ExplorerException("No drive item found matching: " + path);
		}
		return ret;
	}

	@POST
	@Path("download")
	@Produces(MediaType.APPLICATION_OCTET_STREAM)
	@Consumes("application/json")
	public Response downloadUsingStreams(
			final DownloadSpec spec,
			@Context HttpServletRequest request,
			@Context HttpServletResponse resp) {
		Response response = null;

		String path = spec.getPath();
		boolean isPreview = spec.isPreview();
		ActionAudit action = this.auditAction(isPreview ? "preview" : "download", path,
				spec.getDriveId(), PENDING);

		int sourceDriveId = spec.getDriveId();
		String sourcePath = spec.getPath();
//		String userName = request.getUserPrincipal().getName();
		Drive sourceDrive = null;

		try {
			logRequest(request, spec);
			sourceDrive = getDrive(sourceDriveId);

			if (sourceDrive == null) {
				return populateResponseOnMissingInput("No source drive provided.");
			}

			if (assertDriveItemAccess(path, sourceDrive, AccessLevel.Read)) {

				try {
					DriveType driveType = sourceDrive.getDriveType();
					StorageProvider provider =
							StorageProviderFactory.getProvider(driveType, request);

					if (isPreview) {  // TODO use streams
						DriveItem driveItem = provider.getDriveItem(sourceDrive, path);

						if (driveItem != null) {
							long fileSize = driveItem.getFileSize();
							BasicFile ret = preview(provider, sourceDrive, fileSize, spec, action);
							long byteCount = ret.length();
							recordSuccess(action, byteCount);
							response = populateSuccessResponse(ret);
						}
						else {
							throw new ExplorerException("No drive item found matching: " + path);
						}
					}
					else {
						response = downloadFileUsingStreams(resp, provider, sourceDrive, sourcePath, action);
					}
				} catch (Exception e) {
					recordException(action, e);
					populateResponseOnException(action);
				}
			}
			else { // no drive item access
				recordUnauthorized(action);
				response = populateUnauthorizedResponse(action);
			}
		} catch (AuthorizationException e) {
			String msg = "Download aborted: " + e.getMessage();
			logger.log(Level.WARNING, msg , e);
			recordUnauthorized(action);
			populateResponseOnException(action);
		} catch (IOException e) {
			String msg = "Download aborted: " + e.getMessage();
			logger.log(Level.WARNING, msg , e);
			recordException(action, e);
			populateResponseOnException(action);
		}
		return response;
	}

	Response downloadFileUsingStreams(HttpServletResponse resp, StorageProvider provider, Drive sourceDrive,
									  String sourcePath, ActionAudit action) throws IOException {
		Response response;
		try (InputStream inputStream = new ThrottledInputStream(provider.getInputStream(sourceDrive, sourcePath));
			 OutputStream outputStream = resp.getOutputStream()) {

			long byteCount = IOUtils.copyLarge(inputStream, outputStream);
			logger.info("Copied " + byteCount + " from " + sourcePath);
			outputStream.flush();
			recordSuccess(action, byteCount);
			response = populateSuccessResponse();
		}
		return response;
	}


	/**
	 * extracts files from a file archive
	 *
	 * @param locationSpec the metadata for the drive ID and the file location
	 * @return success/failure response object
	 */
	@POST
	@Path("extract")
	public Response extractFile(FileLocationSpec locationSpec, @Context HttpServletRequest request,
								@Context HttpServletResponse resp) {
		Response response = null;
		int driveId = locationSpec.getDriveId();

		// get information about the file being decompressed
		String path = locationSpec.getPath();
		String oldFileName = path.contains("/")
				? path.substring(path.lastIndexOf("/") + 1)
				: path;
		String extension = getExtension(oldFileName);

		// remove the file extension, e.g., .zip or .gz
		String newFileName = oldFileName.substring(0, oldFileName.lastIndexOf("."));

		// the path of the uncompressed file; its is NOT YET decompressed
		String newFileTempPath =
				addLastSlash(FileUtil.TMP_DIR_PROPERTY_VALUE) +
				FileUtil.MDACA_PREFIX + newFileName;

		ActionAudit action = auditAction("extract", path, driveId, PENDING);
		File downloadedFile = null;
		File decompressedFile = null;

		try {
			Drive drive = getDrive(driveId);

			if (drive == null) {
				throw new MissingArgumentException("No drive found for driveID: " + driveId);
			}

			if (assertDriveItemAccess(path, drive, AccessLevel.Modify)) {

				StorageProvider provider =
						StorageProviderFactory.getProvider(drive.getDriveType(), request);

				// For a .Z file we can decompress using an input stream
				// from the provider
				if ("Z".equals(extension)) {
//					decompressedFile = compressedFile.uncompressZArchive(newFileTempPath);
					decompressedFile = extractZUsingTemp(path, newFileTempPath, drive, provider);
				}
				// For a .gzip file we can decompress using an input stream
				// from the provider
				else if (extension != null && "gz".equals(extension.toLowerCase())) {
//					decompressedFile = compressedFile.unGzipFile(newFileTempPath);
					decompressedFile = extractGzUsingTemp(path, newFileTempPath, drive, provider);
				}
				// For all other types download into a temp file and work
				// on that.
				else {
					downloadedFile = provider.download(drive, path);
					downloadedFile.deleteOnExit();

					String extractPath = downloadedFile.getCanonicalPath().replaceAll("\\\\", "/");
					BasicFile compressedFile = new BasicFile(extractPath);

					switch (extension) {
						case "tar":
							decompressedFile = compressedFile.untarFile(newFileTempPath);
							break;
						case "ear":
						case "jar":
						case "war":
						case "zip":
						case "ZIP":
							decompressedFile = compressedFile.unZipFile(newFileTempPath);
							break;
						default:
							throw new ExplorerException(
									"extract is only supported for .ear, .jar, .gz, .tar, .war, .Z, and .zip extensions;"
											+ " instead found \"" + extension + "\"", true);
					}
				}
				decompressedFile.deleteOnExit();

				String destPath = getParentFolder(path);
				destPath = addLastSlash(destPath) + newFileName;

				if (decompressedFile.isDirectory()) {
					provider.uploadDirectory(drive, destPath, decompressedFile);
				} else {
					// if parent folder IS empty, do NOT add a backslash to the path
					provider.upload(drive, destPath, decompressedFile);
				}
				recordSuccess(action);
			} else {
				recordUnauthorized(action);
				return this.populateUnauthorizedResponse(action);
			}

			response = populateSuccessResponse();
		} catch (AuthorizationException e) {
			logger.log(Level.SEVERE, e.getMessage(), e);
			recordUnauthorized(action);
			response = populateUnauthorizedResponse(action);
		} catch (Exception e) {
			logger.log(Level.WARNING, e.getMessage(), e);
			recordException(action, e);
			response = populateResponseOnException(action);
		}
		finally {
			if (downloadedFile != null && downloadedFile.exists()) {
				if (!FileUtils.deleteQuietly(downloadedFile)) {
					logger.info("Unable to delete downloaded temp file " +
								downloadedFile.getName());
				}
			}
			if (decompressedFile != null && decompressedFile.exists()) {
				if (!FileUtils.deleteQuietly(decompressedFile)) {
					logger.info("Unable to delete decompressed temp file " +
								decompressedFile.getName());
				}
			}
		}
		return response;
	}

	File extractGzUsingTemp(String path, String newFileTempPath, Drive drive, StorageProvider provider)
			throws IOException {
		File decompressedFile;
		try (InputStream inputStream = new ThrottledInputStream(provider.getInputStream(drive, path));
				InputStream in = new GZIPInputStream(inputStream);
				OutputStream out = Files.newOutputStream(Paths.get(newFileTempPath))) {
		    FileUtil.transferBytes(in, out, 1024);
		    decompressedFile = new File(newFileTempPath);
		}
		return decompressedFile;
	}

	File extractZUsingTemp(String path, String newFileTempPath, Drive drive, StorageProvider provider)
			throws IOException {
		File decompressedFile;
		try (InputStream inputStream = new ThrottledInputStream(provider.getInputStream(drive, path));
				InputStream in = new ZCompressorInputStream(inputStream);
				OutputStream out = Files.newOutputStream(Paths.get(newFileTempPath))) {
			FileUtil.transferBytes(in, out, 1024);
		    decompressedFile = new File(newFileTempPath);
		}
		return decompressedFile;
	}

	/**
	 * changes the storage class of a given drive item
	 *
	 * @param storageClassSpec the metadata for the drive ID and the file location
	 * @return success/failure response object
	 */
	@POST
	@Path("updateStorageClass")
	public Response updateStorageClass(StorageClassSpec storageClassSpec, @Context HttpServletRequest request,
									   @Context HttpServletResponse resp) {

		int driveId = storageClassSpec.getDriveId();
		Drive drive = null;
		try {

			drive = getDrive(driveId);
		} catch (Exception e) {
			boolean isUnauthorized = e instanceof AuthorizationException;
			logger.log(Level.WARNING, e.getMessage(), e);

			List<ActionAudit> audits = new ArrayList<>();
			for (DriveItem item : storageClassSpec.getDriveItems()) {
				String path = item.getPath();

				// Provide audit data for each delete attempt
				ActionAudit audit = this.auditAction("updateStorageClass", path, driveId, isUnauthorized ? UNAUTHORIZED : ERROR);
				audits.add(audit);
				if (isUnauthorized) {
					recordUnauthorized(audit, e);
				} else {
					recordException(audit, e);
				}
			}
			return isUnauthorized ? populateUnauthorizedResponse(audits) :
					populateResponseOnException(audits);
		}

		List<ActionAudit> audits = new ArrayList<>();

		for (DriveItem driveItem : storageClassSpec.getDriveItems()) {
			String path = driveItem.getPath();

			StorageClass oldStorageClass = driveItem.getStorageClass();
			StorageClass newStorageClass = storageClassSpec.getNewStorageClass();

			ActionAudit action = this.auditAction("updateStorageClass", path,
					storageClassSpec.getDriveId(), PENDING, oldStorageClass, newStorageClass);
			audits.add(action);

			if (assertDriveItemAccess(path, drive, AccessLevel.Archive)) {

				try {
					StorageProvider storageProvider =
							StorageProviderFactory.getProvider(drive.getDriveType(), request);

					if (storageProvider instanceof CloudStorageProvider) {
						CloudStorageProvider provider = (CloudStorageProvider) storageProvider;

						provider.updateStorageClass(drive, path, newStorageClass);
					} else {
						throw new ExplorerException(
								"Expected a cloud storage provider, but instead received type: "
										+ storageProvider.getClass());
					}

					recordSuccess(action);

				} catch (Exception e) {
					recordException(action, e);
				}
			} else {
				recordUnauthorized(action);
			}
		}

		return populateSuccessResponse(new ActionAuditResponse(audits));
	}

	/**
	 * changes the storage class of a given drive item
	 *
	 * @param storageClassSpec the metadata for the drive ID and the file location
	 * @return success/failure response object
	 */
	@POST
	@Path("restore")
	public Response restore(RestoreSpec restoreSpec, @Context HttpServletRequest request,
							@Context HttpServletResponse resp) {
		Response response = null;

		ActionAudit action = this.auditAction("restore", restoreSpec.getPath(), restoreSpec.getDriveId(),
				PENDING);
		try {

			String driveFilePath = restoreSpec.getPath();
			int driveId = restoreSpec.getDriveId();
			Drive drive = getDrive(driveId);

			if (drive == null) {
				return populateResponseOnMissingInput("No drive found for driveID: " + driveId);
			}

			if (assertDriveItemAccess(driveFilePath, drive, AccessLevel.Restore) == true) {

				StorageProvider storageProvider =
						StorageProviderFactory.getProvider(drive.getDriveType(), request);

				if (storageProvider instanceof RestorableCloudStorageProvider) {
					((RestorableCloudStorageProvider) storageProvider).restore(drive, driveFilePath,
							restoreSpec.getDaysExpiration());
				} else {
					throw new ExplorerException(
							"Expected a restorable cloud storage provider, but instead received type: "
									+ storageProvider.getClass());
				}
				recordSuccess(action);
			} else {
				recordUnauthorized(action);
				return this.populateUnauthorizedResponse(action);
			}

			response = populateSuccessResponse();
		} catch (AuthorizationException e) {
			logger.log(Level.SEVERE, e.getMessage(), e);
			recordUnauthorized(action);
			response = populateUnauthorizedResponse(action);
		} catch (Exception e) {
			recordException(action, e);
			logger.log(Level.WARNING, e.getMessage(), e);
			return populateResponseOnException(action);
		}
		return response;
	}


}
