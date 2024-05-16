package com.spinsys.mdaca.storage.explorer.model.http;

public class FileLocationSpec {

	/** A unique identifier for the drive. */
	protected int driveId;
	
	/** A string representing the source folder from which to start a
	 * file system operation, e.g., "/users".  
	 */
	protected String path;
	

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
	
	@Override
	public String toString() {
		return "FileLocationSpec [driveId=" + driveId + ", path=" + path + "]";
	}
}
