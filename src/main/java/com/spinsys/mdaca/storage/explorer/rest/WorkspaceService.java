package com.spinsys.mdaca.storage.explorer.rest;

import com.spinsys.mdaca.storage.explorer.persistence.ActionAudit;
import com.spinsys.mdaca.storage.explorer.persistence.WorkspaceConfig;

import org.apache.commons.lang3.exception.ExceptionUtils;

import javax.ws.rs.GET;
import javax.ws.rs.OPTIONS;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;
import java.util.List;

@Path("workspaces")
public class WorkspaceService extends BaseService {

    public WorkspaceService() {
    }
    

	@OPTIONS
	@Path("list")
	public Response listWorkspacesOPTIONS() {
		return populateSuccessResponse();
	}

	@GET
	@Path("list")
	public Response listWorkspaces() {
		List<WorkspaceConfig> result = entityManager
				.createQuery("from WorkspaceConfig WHERE workspaceUser = :workspaceuser", WorkspaceConfig.class)
				.setParameter("workspaceuser", getCurrentUsername())
				.getResultList();

		return populateSuccessResponse(result);
	}
	
	@OPTIONS
	@Path("create")
	public Response createWorkspaceOPTIONS() {
		return populateSuccessResponse();
	}

    @PUT
    @Path("create")
    public Response createWorkspace(final WorkspaceConfig config) {
		try {
			config.setWorkspaceUser(getCurrentUsername());
			config.setWorkspaceId(0);

			utx.begin();
			entityManager.persist(config);
			utx.commit();
		} catch (Exception e) {
			ActionAudit audit = recordWorkspaceFailure("createWorkspace", config, e);
			return populateResponseOnException(audit);
		}

		recordWorkspaceSuccess("createWorkspace", config);
		return populateSuccessResponse(config);
    }
    

	@OPTIONS
	@Path("update")
	public Response updateWorkspaceOPTIONS() {
		return populateSuccessResponse();
	}

    @POST
    @Path("update")
    public Response updateWorkspace(final WorkspaceConfig config) {
		WorkspaceConfig result = null;
		
		try {
			config.setWorkspaceUser(getCurrentUsername());

			utx.begin();
			result = entityManager.merge(config);
			utx.commit();

			recordWorkspaceSuccess("updateWorkspace", config);
		} catch (Exception e) {
			ActionAudit audit = recordWorkspaceFailure("updateWorkspace", config, e);
			return populateResponseOnException(audit);
		}
		return populateSuccessResponse(result);
    }


	@OPTIONS
	@Path("delete")
	public Response deleteWorkspaceOPTIONS() {
		return populateSuccessResponse();
	}

    @POST
    @Path("delete")
    public Response deleteWorkspace(final WorkspaceConfig config) {
		try {
			utx.begin();
			entityManager.remove(entityManager.contains(config) ? config : entityManager.merge(config));
			utx.commit();
			recordWorkspaceSuccess("deleteWorkspace", config);
		}
		catch (Exception e) {
			ActionAudit audit = recordWorkspaceFailure("deleteWorkspace", config, e);
			return populateResponseOnException(audit);
		}
		return populateSuccessResponse();
    }
    
	ActionAudit recordWorkspaceSuccess(String action, final WorkspaceConfig config) {
		String status = ActionAudit.SUCCESS;
		ActionAudit auditAction = auditWorkspaceAction(action, status, config, null);
		return auditAction;
	}

	ActionAudit recordWorkspaceFailure(String action, final WorkspaceConfig config, Exception e) {
		String status = ActionAudit.ERROR;
		ActionAudit auditAction = auditWorkspaceAction(action, status, config, e);
		return auditAction;
	}

	ActionAudit auditWorkspaceAction(String action, String status, final WorkspaceConfig config, Exception e) {
		int leftDriveId = config.getLeftDriveId();
		int rightDriveId = config.getRightDriveId();
		String workspaceName = config.getWorkspaceName();
		String stackTrace = (e == null) ? null : ExceptionUtils.getStackTrace(e);
		String eMsg = (e == null) ? null : e.getMessage();
		ActionAudit auditAction =
				auditAction(action, workspaceName, leftDriveId, status, null, rightDriveId, eMsg, stackTrace );
		return auditAction;
	}

}
