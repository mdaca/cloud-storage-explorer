package com.spinsys.mdaca.storage.explorer.model.http;

import java.util.List;

import com.spinsys.mdaca.storage.explorer.persistence.ActionAudit;

public class UserResponse {
	private String name;
	private boolean admin;
	private boolean driveAdmin;
	private List<ActionAudit> actionAudits;
	
	public boolean isAdmin() {
		return admin;
	}
	public void setAdmin(boolean admin) {
		this.admin = admin;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public List<ActionAudit> getActionAudits() {
		return actionAudits;
	}
	public void setActionAudits(List<ActionAudit> actionAudits) {
		this.actionAudits = actionAudits;
	}

	public boolean isDriveAdmin() {
		return driveAdmin;
	}

	public void setDriveAdmin(boolean driveAdmin) {
		this.driveAdmin = driveAdmin;
	}

}
