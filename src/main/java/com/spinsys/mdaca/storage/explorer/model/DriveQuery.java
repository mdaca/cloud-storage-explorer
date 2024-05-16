package com.spinsys.mdaca.storage.explorer.model;

import com.spinsys.mdaca.storage.explorer.io.PathProcessor;

import static com.spinsys.mdaca.storage.explorer.io.PathProcessor.isRoot;

public class DriveQuery {

	/** A unique identifier for the drive. */
	private int driveId;
	
	/** A string representing a regular expression for file
	 *  names to match, e.g., "*.csv". */
	private String searchPattern;
	
	/** A string representing the folder from which to start a
	 * file system operation, e.g., "/users".  
	 */
	private String startPath;
	
	/** This flag indicates whether or not to process subdirectories
	 * (recursively). */
	private boolean recursive = false;

	/** Includes the placeholder file in the search
	 */
	private boolean usesPlaceholder;

    public DriveQuery() {

    }

    public DriveQuery(String startPath) {
        this.startPath = startPath;
    }

	public boolean isRecursive() {
		return recursive;
	}

	public void setRecursive(boolean recursive) {
		this.recursive = recursive;
	}

	public int getDriveId() {
		return driveId;
	}

	public void setDriveId(int driveId) {
		this.driveId = driveId;
	}

	public String getStartPath() {
		return startPath;
	}

	public void setStartPath(String startPath) {
		this.startPath = startPath;
	}

	public String getSearchPattern() {
		return searchPattern;
	}

	public void setSearchPattern(String searchPath) {
		this.searchPattern = searchPath;
	}

	public boolean hasSearchPattern() {
		return searchPattern != null && !"".equals(searchPattern);
	}

	public boolean hasStartPath() {
		return !isRoot(startPath);
	}

	public boolean getUsesPlaceholder() {
		return usesPlaceholder;
	}

	public void setUsesPlaceholder(boolean usesPlaceholder) {
		this.usesPlaceholder = usesPlaceholder;
	}

	@Override
	public String toString() {
		return "DriveQuery [driveId=" + driveId + ", searchPattern=" + searchPattern + ", startPath=" + startPath
				+ ", recursive=" + recursive + "]";
	}

	public boolean hasFilters() {
		//if isRecursive is true, all drive items are returned;
		//if it is false, only the drive items at the start path are collected
		boolean needsToTruncate = !isRecursive();

		return hasStartPath() || hasSearchPattern() || needsToTruncate;
	}

	/**
	 * @param driveItem the drive item to test against this DriveQuery
	 * @return true IFF the drive item fields satisfy the filters in this DriveQuery
	 */
	public boolean isIncluded(DriveItem driveItem) {
		if (driveItem.isRoot()) {
			return false;
		}

		String parentFolderPath = PathProcessor.addBothSlashes(driveItem.getParentFolderPath());
		String queryStartPath = (getStartPath() == null) ?
				PathProcessor.GUI_SEP : PathProcessor.addBothSlashes(getStartPath());
		if (!recursive && !queryStartPath.equals(parentFolderPath)) {
			return false;
		}

		//skip the drive items where the start path is the current path
		if (hasStartPath() && driveItem.matchesPath(queryStartPath)) {
			return false;
		}

		if (driveItem.isPlaceholderFile() && !usesPlaceholder) {
			return false;
		}

		if (hasStartPath() && !driveItem.isInsideFolder(queryStartPath)) {
			return false;
		}

		if (hasSearchPattern() && !driveItem.getPath().matches(searchPattern)) {
			return false;
		}

		return true;
	}

}
