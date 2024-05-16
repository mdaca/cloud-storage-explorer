package com.spinsys.mdaca.storage.explorer.rest;

import com.spinsys.mdaca.storage.explorer.model.enumeration.UserRole;
import com.spinsys.mdaca.storage.explorer.model.http.AppInfoResponse;
import com.spinsys.mdaca.storage.explorer.model.http.UserResponse;
import com.spinsys.mdaca.storage.explorer.persistence.ActionAudit;
import com.spinsys.mdaca.storage.explorer.persistence.AppConfig;

import javax.persistence.TypedQuery;
import javax.servlet.http.HttpServletRequest;
import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.ws.rs.GET;
import javax.ws.rs.OPTIONS;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.Base64;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.spinsys.mdaca.storage.explorer.model.enumeration.UserRole.*;

@Path("users")
public class UserService extends BaseService {

	public UserService() {
		
	}

	public static final Logger logger =
			Logger.getLogger("com.spinsys.mdaca.storage.explorer.rest.UserService");


	@OPTIONS
	@Path("info")
	public Response userInfoOPTIONS() {
		return populateSuccessResponse();
	}
	
	@GET
	@Path("abort")
	public Response Abort(@Context HttpServletRequest request) {

		int actionAuditId = Integer.parseInt(request.getParameter("auditId"));

		beginTransaction();
		
		ActionAudit item = entityManager.find(ActionAudit.class, actionAuditId);
		
		if(item.getUsername().compareTo(getCurrentUsername()) != 0) {
			return populateUnauthorizedResponse(new ActionAudit());
		}
		
		item.setStatus("A");
		item.setUpdated(new Date());
		
		entityManager.merge(item);

		try {
			if (utx.getStatus() != 6) {
				utx.commit();
			}
		} catch (SecurityException | IllegalStateException |
				RollbackException | HeuristicMixedException |
				HeuristicRollbackException | SystemException e) {
			logger.log(Level.WARNING, e.getMessage(), e);
		}
		
		return populateSuccessResponse();
		
	}

	@GET
	@Path("in-progress")
	public Response InProgress(@Context HttpServletRequest request) {
		UserResponse resp = new UserResponse();
		String userName = request.getUserPrincipal().getName();
		resp.setName(userName);

		TypedQuery<ActionAudit> query = entityManager.createQuery("from ActionAudit where ACTION_USERNAME = :ACTION_USERNAME and (STATUS = 'P') and ACTION_PATH = 'task'  ORDER BY CREATED desc", ActionAudit.class);
		TypedQuery<ActionAudit> queryWithParameter = query.setParameter("ACTION_USERNAME", userName).setMaxResults(50);
		List<ActionAudit> result = queryWithParameter.getResultList();
		
		//do not send exception details to regular users
		for(ActionAudit aud : result) {
			aud.setStackTrace(null);

			if (!aud.isUseMessage()) {
				aud.setMessage(null);
			}
			if (aud.getDrive() != null) {
				aud.getDrive().voidMappedClasses();
			}
		}
		
		resp.setActionAudits(result);

		UserRole userRole = getUserRole();

		resp.setAdmin(userRole.equals(ADMIN));
		resp.setDriveAdmin(userRole.equals(DRIVE_ADMIN));

		return populateSuccessResponse(resp);
		
	}
	
	@GET
	@Path("info")
    public Response userInfo(@Context HttpServletRequest request) {
		UserResponse resp = new UserResponse();
		String userName = request.getUserPrincipal().getName();
		resp.setName(userName);

		TypedQuery<ActionAudit> query = entityManager.createQuery("from ActionAudit where ACTION_USERNAME = :ACTION_USERNAME ORDER BY CREATED desc", ActionAudit.class);
		TypedQuery<ActionAudit> queryWithParameter = query.setParameter("ACTION_USERNAME", userName).setMaxResults(50);
		List<ActionAudit> result = queryWithParameter.getResultList();
		
		//do not send exception details to regular users
		for(ActionAudit aud : result) {
			aud.setStackTrace(null);

			if (!aud.isUseMessage()) {
				aud.setMessage(null);
			}
			if (aud.getDrive() != null) {
				aud.getDrive().voidMappedClasses();
			}
		}
		
		resp.setActionAudits(result);

		UserRole userRole = getUserRole();

		resp.setAdmin(userRole.equals(ADMIN));
		resp.setDriveAdmin(userRole.equals(DRIVE_ADMIN));

		return populateSuccessResponse(resp);
    }

	@GET
	@Path("app/info")
    public Response appInfo(@Context HttpServletRequest request) {
		AppInfoResponse resp = new AppInfoResponse();
		
		List<AppConfig> result = entityManager.createQuery( "from AppConfig WHERE CONFIG_KEY = 'LICENSE_KEY'", AppConfig.class ).getResultList();
		
		if(result.size() == 1) {
			
			try {
				String decoded = new String(Base64.getDecoder().decode(result.get(0).getConfigValue()), "UTF-8");

				String[] raw = decoded.split("\\|@\\|");
				
				resp.setLicensedBy(raw[0]);
				resp.setLicenseType(raw[1]);
				resp.setLicenseExpiration(raw[2]);
				
			} catch (UnsupportedEncodingException e) {
				logger.log(Level.WARNING, e.getMessage(), e);
			}
			
		}
		
		resp.setVersionNumber(getVersion());
		
		return populateSuccessResponse(resp);
    	
    }
	
	public synchronized String getVersion() {
	    String version = null;

	    // try to load from maven properties first
	    try {
	        Properties p = new Properties();
	        InputStream is = getClass().getResourceAsStream("/META-INF/maven/mdaca-storage-explorer-services/mdaca-storage-explorer-services/pom.properties");
	        if (is != null) {
	            p.load(is);
	            version = p.getProperty("version", "");
	        }
	    } catch (Exception e) {
	        // ignore
	    }

	    // fallback to using Java API
	    if (version == null) {
	        Package aPackage = getClass().getPackage();
	        if (aPackage != null) {
	            version = aPackage.getImplementationVersion();
	            if (version == null) {
	                version = aPackage.getSpecificationVersion();
	            }
	        }
	    }

	    if (version == null) {
	        // we could not compute the version so use a blank
	        version = "";
	    }

	    return version;
	} 
}
