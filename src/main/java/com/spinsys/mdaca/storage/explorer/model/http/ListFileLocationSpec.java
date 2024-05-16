package com.spinsys.mdaca.storage.explorer.model.http;

import java.util.List;

public class ListFileLocationSpec {

	private int driveId;

	private List<String> paths;

	public int getDriveId() {
		return driveId;
	}

	public void setDriveId(int driveId) {
		this.driveId = driveId;
	}

	public List<String> getPaths() {
		return paths;
	}

	public void setPaths(List<String> paths) {
		this.paths = paths;
	}

	public String toString() {
		return "[drive: " + driveId + ", path(s):\n        " + String.join("\n        ", paths) + "]";
	}

	public String getPathsAsString() {
		return (paths == null) ? "[]" : paths.toString();
	}

}
