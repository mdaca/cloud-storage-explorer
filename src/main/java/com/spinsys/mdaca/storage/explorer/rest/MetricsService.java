package com.spinsys.mdaca.storage.explorer.rest;

import static com.spinsys.mdaca.storage.explorer.persistence.ActionAudit.ERROR;
import static com.spinsys.mdaca.storage.explorer.persistence.ActionAudit.UNAUTHORIZED;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.OPTIONS;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import org.apache.commons.lang3.exception.ExceptionUtils;

import com.spinsys.mdaca.storage.explorer.model.DriveItem;
import com.spinsys.mdaca.storage.explorer.model.DriveQuery;
import com.spinsys.mdaca.storage.explorer.model.dto.DriveMemoryUsageDTO;
import com.spinsys.mdaca.storage.explorer.model.dto.FolderMemoryUsageDTO;
import com.spinsys.mdaca.storage.explorer.model.enumeration.AccessLevel;
import com.spinsys.mdaca.storage.explorer.model.enumeration.DriveType;
import com.spinsys.mdaca.storage.explorer.model.exception.AuthorizationException;
import com.spinsys.mdaca.storage.explorer.model.exception.ExplorerException;
import com.spinsys.mdaca.storage.explorer.model.exception.MissingInputException;
import com.spinsys.mdaca.storage.explorer.model.http.DriveUsageHistorySpec;
import com.spinsys.mdaca.storage.explorer.persistence.ActionAudit;
import com.spinsys.mdaca.storage.explorer.persistence.Drive;
import com.spinsys.mdaca.storage.explorer.persistence.DriveMemoryUsageHistory;
import com.spinsys.mdaca.storage.explorer.persistence.MemoryUsage;
import com.spinsys.mdaca.storage.explorer.provider.StorageProvider;
import com.spinsys.mdaca.storage.explorer.provider.StorageProviderFactory;

/**
 * This class provides various metrics about CSE and
 * the services and resources it uses.
 */
@Path("metrics")
public class MetricsService extends DriveService {

	private static final Logger logger =
			Logger.getLogger("com.spinsys.mdaca.storage.explorer.rest.MetricsService");

	/**
	 * @return for the input directory, a list of all subfolder-usage pairs
	 */
	@POST
	@Path("disk/folder/subfolders")
	public Response folderMemoryUsage(final DriveQuery query, @Context HttpServletRequest request)
	{
		Response response = null;
		long start = System.currentTimeMillis();
		try {
			logRequest(request, query);
	
			if (query == null) {
				throw new ExplorerException("getFolderUsage - null query");
			}
			int driveId = query.getDriveId();
			Drive drive = getDrive(driveId);
	
			if (drive == null) {
				throw new MissingInputException("No source drive provided.");
			}
	
			String startPath = query.getStartPath();
			
			if (!assertDriveItemAccess(startPath, drive, AccessLevel.Read)) {
				throw new AuthorizationException(
						"User does not have read permissions to access " +
								startPath + " on drive " + drive);
			}
	
			DriveType driveType = drive.getDriveType();
			StorageProvider provider = StorageProviderFactory.getProvider(driveType, request);
			query.setRecursive(false); // just 1 level
			List<DriveItem> driveItems = findDriveItems(query, drive, provider);
			List<FolderMemoryUsageDTO> usageDTOs = buildFolderUsageDTOs(driveId, driveItems);
			response = populateSuccessResponse(usageDTOs);
		} catch (AuthorizationException e) {
			String msg = e.getMessage();
			String stackTrace = ExceptionUtils.getStackTrace(e);
			logger.log(Level.SEVERE, e.getMessage(), e);
			ActionAudit action = auditAction("folderMemoryUsage", query.getStartPath(),
					query.getDriveId(), UNAUTHORIZED, null, 0, msg, stackTrace);
			response = populateUnauthorizedResponse(action);
		} catch (Exception e) {
			String msg = "search pattern: " + query.getSearchPattern();
			String stackTrace = ExceptionUtils.getStackTrace(e);
			logger.log(Level.WARNING, e.getMessage(), e);
			ActionAudit action = auditAction("folderMemoryUsage", query.getStartPath(),
					query.getDriveId(), ERROR, null, 0, msg, stackTrace);
			response = populateResponseOnException(action);
		}
		logger.info("folderMemoryUsage took " +
				(System.currentTimeMillis() - start ) + " ms.");
		return response;
	}

	@OPTIONS
	@Path("disk/folder/subfolders")
	public Response folderMemoryUsageOPTIONS() {
		return populateSuccessResponse();
	}

	/**
	 * @return the amount of memory used for the input directory,
	 * including the files immediately in it and those in all
	 * subdirectories, recursively.
	 */
	@POST
	@Path("disk/folder/total")
	public Response folderMemoryUsageTotal(final DriveQuery query, @Context HttpServletRequest request)
	{
		Response response = null;
		try {
			logRequest(request, query);
	
			if (query == null) {
				throw new ExplorerException("folderMemoryUsageTotal - null query");
			}
			int driveId = query.getDriveId();
			Drive drive = getDrive(driveId);
	
			if (drive == null) {
				throw new MissingInputException("No source drive provided.");
			}
	
			String startPath = query.getStartPath();
			
			if (!assertDriveItemAccess(startPath, drive, AccessLevel.Read)) {
				throw new AuthorizationException(
						"User does not have read permissions to access " +
								startPath + " on drive " + drive);
			}
			Long numBytes = MemoryUsage.getFolderUsage(driveId, startPath, entityManager);
			response = populateSuccessResponse(numBytes);
		} catch (AuthorizationException e) {
			String msg = e.getMessage();
			String stackTrace = ExceptionUtils.getStackTrace(e);
			logger.log(Level.SEVERE, e.getMessage(), e);
			ActionAudit action = auditAction("folderMemoryUsageTotal", query.getStartPath(),
					query.getDriveId(), UNAUTHORIZED, null, 0, msg, stackTrace);
			response = populateUnauthorizedResponse(action);
		} catch (Exception e) {
			String msg = "search pattern: " + query.getSearchPattern();
			String stackTrace = ExceptionUtils.getStackTrace(e);
			logger.log(Level.WARNING, e.getMessage(), e);
			ActionAudit action = auditAction("folderMemoryUsageTotal", query.getStartPath(),
					query.getDriveId(), ERROR, null, 0, msg, stackTrace);
			response = populateResponseOnException(action);
		}
		return response;
	}

	@OPTIONS
	@Path("disk/folder/total")
	public Response folderMemoryUsageTotalOPTIONS() {
		return populateSuccessResponse();
	}

	/**
	 * @return a historical record of the amount of memory
	 *  used for the input drive
	 */
	@POST
	@Path("disk/drive/history")
	public Response driveMemoryUsageHistory(DriveUsageHistorySpec spec,
			@Context HttpServletRequest request) {
		Response response = null;
		long start = System.currentTimeMillis();
	
		Date startDate = spec.getStartDate() != null
				 ? spec.getStartDate() : new Date(0);
		
		Date endDate = spec.getEndDate() != null
				 ? spec.getEndDate() : new Date();
		
		// If the end date corresponds to the Unix Epoch, and it's
		// not later than the start date,
		// assume that the end date wasn't specified and change the
		// end date to now.
		if (endDate.equals(new Date(0)) && startDate.compareTo(endDate) >= 0) {
			endDate = new Date();
		}
		
		int driveId = spec.getDriveId();
	
		try {
			logRequest(request, spec);
			Drive drive = getDrive(driveId);
	
			if (drive == null) {
				throw new MissingInputException("No source drive provided.");
			}
	
			if (!assertDriveItemAccess("/", drive, AccessLevel.Read)) {
				throw new AuthorizationException(
						"User does not have read permissions to access drive " + drive);
			}
	
			int numDesired = (spec.getResponseSize() > 0)
					? spec.getResponseSize() : Integer.MAX_VALUE;
			List<DriveMemoryUsageHistory> driveUsageHistory =
					DriveMemoryUsageHistory.getDriveUsageHistory(driveId, startDate, endDate, numDesired , getEntityManager());
	
			List<DriveMemoryUsageDTO> history = new ArrayList<>();
	
			// Translate results into DTOs
			for (DriveMemoryUsageHistory usage : driveUsageHistory) {
				DriveMemoryUsageDTO dto = new DriveMemoryUsageDTO();
				dto.setCreated(usage.getCreated());
				dto.setBytes(usage.getBytes());
				history.add(dto);
			}
			response = populateSuccessResponse(history);
		} catch (AuthorizationException e) {
			String msg = e.getMessage();
			String stackTrace = ExceptionUtils.getStackTrace(e);
			logger.log(Level.SEVERE, e.getMessage(), e);
			ActionAudit action = auditAction("driveMemoryUsageHistory", "/",
					driveId, UNAUTHORIZED, null, 0, msg, stackTrace);
			response = populateUnauthorizedResponse(action);
		} catch (Exception e) {
			String stackTrace = ExceptionUtils.getStackTrace(e);
			logger.log(Level.WARNING, e.getMessage(), e);
			ActionAudit action = auditAction("driveMemoryUsageHistory", "/",
					driveId, ERROR, null, 0, e.getMessage(), stackTrace);
			response = populateResponseOnException(action);
		}
		logger.info("driveMemoryUsageHistory took " +
				(System.currentTimeMillis() - start) + " ms.");
		return response;
	}

	@OPTIONS
	@Path("disk/drive/history")
	public Response driveMemoryUsageHistoryOPTIONS() {
		return populateSuccessResponse();
	}

	/**
	 * @return the files using the most memory in this
	 *  directory and its subdirectories.
	 */
	@POST
	@Path("disk/files")
	public Response largeFiles(final DriveQuery query, @Context HttpServletRequest request) {
	
		Response response = null;
		try {
			logRequest(request, query);
	
			if (query == null) {
				throw new ExplorerException("largeFiles - null query");
			}
			int driveId = query.getDriveId();
			Drive drive = getDrive(driveId);
	
			if (drive == null) {
				throw new MissingInputException("No source drive provided.");
			}
	
			String startPath = query.getStartPath();
			
			if (!assertDriveItemAccess(startPath, drive, AccessLevel.Read)) {
				throw new AuthorizationException(
						"User does not have read permissions to access " +
								startPath + " on drive " + drive);
			}
	
			List<MemoryUsage> biggestFiles =
					MemoryUsage.getBiggestFiles(driveId, startPath, 50, entityManager);
			List<FolderMemoryUsageDTO> usageDTOs = new ArrayList<>();
	
			// Get usage for each subfolder and add a corresponding DTO
			for (MemoryUsage item : biggestFiles) {
				FolderMemoryUsageDTO dto = new FolderMemoryUsageDTO();
				dto.setPath(item.getPath());
				dto.setBytes(item.getBytes());
				usageDTOs.add(dto);
			}
			response = populateSuccessResponse(usageDTOs);
		} catch (AuthorizationException e) {
			String msg = e.getMessage();
			String stackTrace = ExceptionUtils.getStackTrace(e);
			logger.log(Level.SEVERE, e.getMessage(), e);
			ActionAudit action = auditAction("largeFiles", query.getStartPath(),
					query.getDriveId(), UNAUTHORIZED, null, 0, msg, stackTrace);
			response = populateUnauthorizedResponse(action);
		} catch (Exception e) {
			String msg = "search pattern: " + query.getSearchPattern();
			String stackTrace = ExceptionUtils.getStackTrace(e);
			logger.log(Level.WARNING, e.getMessage(), e);
			ActionAudit action = auditAction("largeFiles", query.getStartPath(),
					query.getDriveId(), ERROR, null, 0, msg, stackTrace);
			response = populateResponseOnException(action);
		}
		return response;
	}

	@OPTIONS
	@Path("metrics/disk/files")
	public Response largeFilesOPTIONS() {
		return populateSuccessResponse();
	}

}
