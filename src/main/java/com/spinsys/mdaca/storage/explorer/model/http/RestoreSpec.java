package com.spinsys.mdaca.storage.explorer.model.http;

public class RestoreSpec {

	/** A unique identifier for the drive. */
	protected int driveId;
	
	/** A string representing the source folder from which to start a
	 * file system operation, e.g., "/users".  
	 */
	protected String path;
	
	protected int daysExpiration;

	public String getPath() {
		return path;
	}
	public void setPath(String path) {
		this.path = path;
	}
	public int getDriveId() {
		return driveId;
	}
	public void setDriveId(int driveId) {
		this.driveId = driveId;
	}
	
	public int getDaysExpiration( ) {
		return daysExpiration;
	}
	
	public void setDaysExpiration(int daysExpiration) {
		this.daysExpiration = daysExpiration;
	}
	
	@Override
	public String toString() {
		return "FileLocationSpec [driveId=" + driveId + ", path=" + path + "]";
	}
}
