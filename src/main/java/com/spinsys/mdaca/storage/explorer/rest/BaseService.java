package com.spinsys.mdaca.storage.explorer.rest;

import com.google.gson.Gson;
import com.spinsys.mdaca.storage.explorer.model.StorageClass;
import com.spinsys.mdaca.storage.explorer.model.enumeration.UserRole;
import com.spinsys.mdaca.storage.explorer.model.http.ActionAuditResponse;
import com.spinsys.mdaca.storage.explorer.persistence.ActionAudit;
import com.spinsys.mdaca.storage.explorer.persistence.Drive;
import com.spinsys.mdaca.storage.explorer.persistence.TableUtils;
import com.spinsys.mdaca.storage.explorer.model.exception.ExplorerException;

import org.jboss.resteasy.spi.ResteasyProviderFactory;
import org.jboss.resteasy.core.providerfactory.ResteasyProviderFactoryImpl;

import javax.annotation.Resource;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.servlet.http.HttpServletRequest;
import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.UserTransaction;
import javax.ws.rs.core.Response;
import java.security.Principal;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.spinsys.mdaca.storage.explorer.model.enumeration.UserRole.ADMIN;
import static com.spinsys.mdaca.storage.explorer.model.enumeration.UserRole.DRIVE_ADMIN;
import static com.spinsys.mdaca.storage.explorer.model.enumeration.UserRole.USER;
import static com.spinsys.mdaca.storage.explorer.persistence.ActionAudit.ERROR;
import static com.spinsys.mdaca.storage.explorer.persistence.ActionAudit.SUCCESS;
import static com.spinsys.mdaca.storage.explorer.persistence.ActionAudit.UNAUTHORIZED;
import static org.apache.commons.lang3.exception.ExceptionUtils.getStackTrace;

public class BaseService {

	public static final int SUCCESS_STATUS = 200;
    public static final int FORBIDDEN = 403;
	public static final int INTERNAL_SERVER_ERROR = 500;

	public BaseService() {
	}
	
	public BaseService(EntityManager entityManager, UserTransaction utx) {
		this.entityManager = entityManager;
		this.utx = utx;
	}

	private static final Logger logger =
            Logger.getLogger("com.spinsys.mdaca.storage.explorer.rest.BaseService");

	@Resource
	public static UserTransaction utx;

    @PersistenceContext(unitName = TableUtils.STOREXP_PERSISTENT_UNIT)
    public static EntityManager entityManager;

    /** A date formatter to convert input strings into date objects. */
    protected SimpleDateFormat formatter = new SimpleDateFormat("MM/dd/yyyy");

	protected ActionAudit auditAction(
			String action,
			String path, int driveId,
			String status) {

		ActionAudit audit = new ActionAudit();
		Drive drive = entityManager.getReference(Drive.class, driveId);

		audit.setAction(action);
		audit.setPath(path);
		audit.setDrive(drive);
		audit.setStatus(status);

		saveAudit(audit);

		return audit;
	}

	protected ActionAudit auditAction(
			String action,
			String sourcePath, int sourceDriveId,
			String destPath, int destDriveId,
			String status)
	{
		ActionAudit audit = new ActionAudit();
		Drive drive = entityManager.getReference(Drive.class, sourceDriveId);

		audit.setAction(action);
		audit.setPath(sourcePath);
		audit.setDrive(drive);
		audit.setDestPath(destPath);
		audit.setDestDriveId(destDriveId);
		audit.setStatus(status);

		saveAudit(audit);

		return audit;
	}

	protected ActionAudit auditAction(
			String action,
			String sourcePath, int sourceDriveId,
			String status,
			StorageClass oldStorageClass,
			StorageClass newStorageClass)
	{
		ActionAudit audit = new ActionAudit();
		Drive drive = entityManager.getReference(Drive.class, sourceDriveId);

		audit.setAction(action);
		audit.setPath(sourcePath);
		audit.setDrive(drive);
		audit.setStatus(status);
		audit.setOldStorageClass(oldStorageClass.getDisplayText());
		audit.setNewStorageClass(newStorageClass.getDisplayText());

		saveAudit(audit);

		return audit;
	}

	protected ActionAudit auditAction(
			String action,
			String path, int driveId,
			String status,
			String destPath, int destDriveId,
			String message, String stackTrace)
	{
		ActionAudit audit = new ActionAudit();
		Drive drive = entityManager.getReference(Drive.class, driveId);

		audit.setAction(action);
		audit.setPath(path);
		audit.setDrive(drive);
		audit.setDestPath(destPath);
		audit.setDestDriveId(destDriveId);
		audit.setMessage(message);
		audit.setStackTrace(stackTrace);
		audit.setStatus(status);

		saveAudit(audit);

		return audit;
	}

	public static ActionAudit auditAction(
			String action,
			String path, int driveId,
			String status,
			String destPath, int destDriveId,
			String message, String stackTrace, HttpServletRequest request)
	{
		ActionAudit audit = new ActionAudit();
		Drive drive = entityManager.getReference(Drive.class, driveId);

		audit.setAction(action);
		audit.setPath(path);
		audit.setDrive(drive);
		audit.setDestPath(destPath);
		audit.setDestDriveId(destDriveId);
		audit.setMessage(message);
		audit.setStackTrace(stackTrace);
		audit.setStatus(status);

		saveAudit(audit, request);

		return audit;
	}


	public static ActionAudit auditAction(
			String action,
			String path, int driveId,
			String status,
			String destPath, int destDriveId,
			String message, String stackTrace, String username, String ipAddress)
	{
		ActionAudit audit = new ActionAudit();
		Drive drive = entityManager.getReference(Drive.class, driveId);

		audit.setAction(action);
		audit.setPath(path);
		audit.setDrive(drive);
		audit.setDestPath(destPath);
		audit.setDestDriveId(destDriveId);
		audit.setMessage(message);
		audit.setStackTrace(stackTrace);
		audit.setStatus(status);

		saveAudit(audit, username, ipAddress);

		return audit;
	}

	protected ActionAudit getActionAudit(
			String action, String path, int driveId, String destPath,
			int destDriveId, String status)
	{
		ActionAudit audit = null;
		List<ActionAudit> audits = entityManager
				.createQuery("from ActionAudit WHERE" +
						" username = :username and" +
						" action = :action and" +
						" path = :path and" +
						" drive = :drive and " +
						" destPath = :destPath and " +
						" destDriveId = :destDriveId and " +
						" status = :status " +
						" order by created desc"
						, ActionAudit.class)
				.setParameter("username", getCurrentUsername())
				.setParameter("action", action)
				.setParameter("path", path)
				.setParameter("drive", entityManager.getReference(Drive.class, driveId))
				.setParameter("destPath", destPath)
				.setParameter("destDriveId", destDriveId)
				.setParameter("status", status)
				.getResultList();

		// We expect an audit to exist in the database.  Get it.
		if (audits.size() > 0) {
			audit = audits.get(0);
		}
		// If for some reason, no audit already exists in the database, create one.
		else {
			audit = auditAction(action, path, driveId, status, destPath, destDriveId, "", null);
		}
		return audit;
	}
	
	private static void saveAudit(ActionAudit audit, String username, String ipAddress) {
		audit.setIpAddress(ipAddress);
		audit.setUsername(username);

		beginTransaction();
		entityManager.persist(audit);

		try {
			if(utx.getStatus() != 6) {
				utx.commit();
			}
		} catch (Exception e) {
			logger.log(Level.WARNING, e.getMessage(), e);
		}
	}

	private static void saveAudit(ActionAudit audit, HttpServletRequest request) {
		saveAudit(audit, getCurrentUsername(request), getIpAddress(request));
	}

	private static void saveAudit(ActionAudit audit) {
		audit.setIpAddress(getIpAddress());
		audit.setUsername(getCurrentUsername());

		beginTransaction();
		entityManager.persist(audit);

		try {
			if(utx.getStatus() != 6) {
				utx.commit();
			}
		} catch (Exception e) {
			logger.log(Level.WARNING, e.getMessage(), e);
		}
	}

	public static void updateAction(ActionAudit action) {
		beginTransaction();
		ActionAudit merge = entityManager.merge(action);

		try {
			if (utx.getStatus() != 6) {
				utx.commit();
			}
		} catch (SecurityException | IllegalStateException |
				RollbackException | HeuristicMixedException |
				HeuristicRollbackException | SystemException e) {
			logger.log(Level.WARNING, e.getMessage(), e);
		}

		//for serialization; updates the action audit to avoid a lazy init exception
		Drive drive = merge.getDrive();
		drive.voidMappedClasses();

		action.setDrive(drive);
		action.setUpdated(merge.getUpdated());
	}

	public static void recordUnauthorized(ActionAudit action, Exception e, long bytesTransferred) {
		action.setBytesTransferred(bytesTransferred);

		recordUnauthorized(action, e);
	}

	public static void recordUnauthorized(ActionAudit action) {
		action.setStatus(UNAUTHORIZED);
		action.setUseMessage(true);
		updateAction(action);
	}

	public static void recordUnauthorized(ActionAudit action, Exception e) {
		action.setStatus(UNAUTHORIZED);
		action.setMessage(e.getMessage());
		action.setStackTrace(getStackTrace(e));
		action.setUseMessage(true);
		updateAction(action);
	}

	public static void recordSuccess(ActionAudit action, long bytesTransferred) {
		action.setBytesTransferred(bytesTransferred);
		recordSuccess(action);
	}

	static String recordSuccess(ActionAudit action) {
		String status = SUCCESS;
		action.setStatus(status);
		updateAction(action);
		return status;
	}

	public static String recordException(ActionAudit action, Exception e, long bytesTransferred) {
		action.setBytesTransferred(bytesTransferred);

		return recordException(action, e);
	}

	static String recordException(ActionAudit action, Exception e) {
		boolean useMessage = (e instanceof ExplorerException) && ((ExplorerException) e).isUseMessage();

		String message = e.getMessage();
		String status = ERROR;
		action.setStatus(status);
		action.setStackTrace(getStackTrace(e));
		action.setMessage(message);
		action.setUseMessage(useMessage);
		updateAction(action);
		logger.log(Level.WARNING, message, e);
		return status;
	}

	public static void beginTransaction() {
		try {
			if(utx.getStatus() == 6) {
				utx.begin();
			}
		} catch (SystemException | NotSupportedException e) {
			logger.log(Level.WARNING, e.getMessage(), e);
		}
	}

	public static Response populateSuccessResponse() {
		return populateSuccessResponse(null);
	}

	public static Response populateSuccessResponse(List<ActionAudit> audits) {
		return populateSuccessResponse(new ActionAuditResponse(audits));
	}

    public static Response populateSuccessResponse(Object ret) {

            if (ret == null) {
                    return Response.status(SUCCESS_STATUS).type("application/json").header("Access-Control-Allow-Origin", "http://localhost:4200")
                                    .header("Access-Control-Allow-Headers", "origin, content-type, accept, authorization")
                                    .header("Access-Control-Allow-Credentials", "true")
                                    .header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS, HEAD")
                                    .header("Access-Control-Max-Age", "1209600").build();
            } else {
                    return Response.status(SUCCESS_STATUS).type("application/json").entity(ret).header("Access-Control-Allow-Origin", "http://localhost:4200")
                                    .header("Access-Control-Allow-Headers", "origin, content-type, accept, authorization")
                                    .header("Access-Control-Allow-Credentials", "true")
                                    .header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS, HEAD")
                                    .header("Access-Control-Max-Age", "1209600").build();
            }
    }
    
    public static Principal getCurrentUserToken(HttpServletRequest request) {
		Principal userPrincipal =
				(request != null) ? request.getUserPrincipal() : null;
		return userPrincipal;
    }

    public static Principal getCurrentUserToken() {
    	return getCurrentUserToken(getHttpServletRequest());
    }
    
    public static HttpServletRequest request = null;

	public static HttpServletRequest getHttpServletRequest() {
		ResteasyProviderFactory resteasyProviderFactory = ResteasyProviderFactoryImpl.getInstance();
		return request != null ? request : resteasyProviderFactory.getContextData(HttpServletRequest.class);
	}

    public static String getCurrentUsername(HttpServletRequest request) {
    	Principal userToken = getCurrentUserToken(request);
		String name = (userToken != null) ? userToken.getName() : "";
		
		if ("".equals(name)) {
			logger.warning("No name found");
		}
		return name;
    }

    public static String getCurrentUsername() {
    	return getCurrentUsername(getHttpServletRequest());
    }

    public static  String getIpAddress(){
		return getIpAddress(getHttpServletRequest());
	}

    public static  String getIpAddress(HttpServletRequest request){
		return request.getHeader("X-FORWARDED-FOR");
	}

	public static void logRequest(HttpServletRequest request, Object spec) {
		try {
			String url = request.getRequestURL().toString();
			logger.info("Processing request " + url + ", with spec: " + spec);
		} catch(Exception ex) {
			
		}
	}

	public static Response populateResponseOnException(ActionAudit action) {
		return populateResponseOnException(Collections.singletonList(action));
	}

	protected static Response populateResponseOnException(List<ActionAudit> audits) {
    	return getInternalServerErrorResponse(new ActionAuditResponse(audits));
	}

	protected static Response populateUnauthorizedResponse(ActionAudit action) {
    	return populateUnauthorizedResponse(Collections.singletonList(action));
	}

	protected static Response populateUnauthorizedResponse(List<ActionAudit> audits) {
		return populateUnauthorizedResponse(new ActionAuditResponse(audits));
	}

	/**
	 * Return a response indicating that the server has encountered an
	 *  missing input it doesn't know how to handle.
	 * @param msg an explanatory message
	 * @return a response with a 500 return code
	 */
	public Response populateResponseOnMissingInput(String msg) {
		String returnJSON = "{ \"missingInput\": \"" + msg + "\"}";
	    return getInternalServerErrorResponse(returnJSON);
	}


	/**
	 * Return a response indicating that the server has encountered a
	 *  situation it doesn't know how to handle.
	 * @param returnJSON JSON providing an indication of the problem
	 * @return a response with a 500 return code
	 */
	public static Response getInternalServerErrorResponse(Object returnJSON) {
		return Response.status(INTERNAL_SERVER_ERROR).type("application/json").entity(returnJSON).header("Access-Control-Allow-Origin", "http://localhost:4200")
	            .header("Access-Control-Allow-Headers", "origin, content-type, accept, authorization")
	            .header("Access-Control-Allow-Credentials", "true")
	            .header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS, HEAD")
	            .header("Access-Control-Max-Age", "1209600").build();
	}

	public static Response populateUnauthorizedResponse(Object returnJSON) {
		return Response.status(FORBIDDEN).type("application/json").entity(returnJSON).header("Access-Control-Allow-Origin", "http://localhost:4200")
	            .header("Access-Control-Allow-Headers", "origin, content-type, accept, authorization")
	            .header("Access-Control-Allow-Credentials", "true")
	            .header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS, HEAD")
	            .header("Access-Control-Max-Age", "1209600").build();
	}


	/**
	 * Assemble composite response and return it
	 * @param problemResponses one or more problems
	 * @return a composite response
	 */
	protected Response consolidateProblems(List<Response> problemResponses) {
		Response compositeResponse = null;

		int numErrors = problemResponses.size();
		if (numErrors == 1) {
			compositeResponse = problemResponses.get(0);
		}
		else if (numErrors > 1) {
			Gson gson = new Gson();
			try {
				String json = gson.toJson(problemResponses);
				compositeResponse = getInternalServerErrorResponse(json);
			}
			catch (Exception e) {
				String msg = "" + numErrors + " errors.  First one: " +
						problemResponses.get(0).getEntity();
				compositeResponse = getInternalServerErrorResponse(msg);
			}
		}
		return compositeResponse;
	}

	public static EntityManager getEntityManager() {
		return entityManager;
	}

	public UserRole getUserRole() {

		if (isInGroup(getRoleName(ADMIN.getGroupName()))) {
			return ADMIN;
		} else if (isInGroup(getRoleName(DRIVE_ADMIN.getGroupName()))) {
			return DRIVE_ADMIN;
		} else {
			return USER;
		}
	}

	public static boolean isInGroup(String groupName, List<String> roles) {
		for(String principal : roles) {
			if(principal.compareToIgnoreCase(groupName) == 0) {
				return true;
			}
		}
		
		return false;
	}
	
	static boolean isInGroup(String groupName) {
		//String roleName = getRoleName(groupName);
		HttpServletRequest request = getHttpServletRequest();
		boolean userInRole = (request != null) && (groupName != null) 
				&& request.isUserInRole(groupName);
		return userInRole;
	}

	private static String getRoleName(String groupKey) {
		Object role = null;
		AppConfigurationEntry[] appConfigurationEntry = Configuration.getConfiguration()
				.getAppConfigurationEntry("spnego-auth");

		if (appConfigurationEntry != null) {
			String loginModuleKey = "com.spinsys.mdaca.storage.explorer.rest.SpnegoLoginModule";

			Optional<AppConfigurationEntry> first = Arrays.stream(appConfigurationEntry)
					.filter(entry -> loginModuleKey.equals(entry.getLoginModuleName()))
					.findFirst();
			
			if (first.isPresent()) {
				AppConfigurationEntry appConfigurationEntry1 = first.get();
				Map<String, ?> options = appConfigurationEntry1.getOptions();
				role = options.get(groupKey);
			}
		}
		
		String roleName =
				(role != null)
				? role.toString()
				: UserRole.getDefaultRoleName(groupKey);
		return roleName;
	}

}
