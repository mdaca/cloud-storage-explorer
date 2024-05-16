package com.spinsys.mdaca.storage.explorer.model.enumeration;

import static com.spinsys.mdaca.storage.explorer.model.enumeration.UserRole.ADMIN;
import static com.spinsys.mdaca.storage.explorer.model.enumeration.UserRole.DRIVE_ADMIN;

public enum UserRole {

    USER("userGroup"),
    DRIVE_ADMIN("driveAdminGroup"),
    ADMIN("adminGroup");

    private final String groupName;

    UserRole(String groupName) {
        this.groupName = groupName;
    }

    public String getGroupName() {
        return groupName;
    }

	public static String getDefaultRoleName(String groupKey) {
		String roleName = null;
		if (ADMIN.getGroupName().equals(groupKey)) {
			roleName = "StorageExplorerAdmins";
		} else if (DRIVE_ADMIN.getGroupName().equals(groupKey)) {
			roleName = "StorageExplorerDriveAdmins";
		} else {
			roleName = "StorageExplorerUsers";
		}
		return roleName;
	}

}
