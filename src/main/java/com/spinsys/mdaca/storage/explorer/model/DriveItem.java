package com.spinsys.mdaca.storage.explorer.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.spinsys.mdaca.storage.explorer.io.FileUtil;
import com.spinsys.mdaca.storage.explorer.io.PathProcessor;
import com.spinsys.mdaca.storage.explorer.model.enumeration.AccessLevel;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.codehaus.jackson.annotate.JsonIgnore;

import java.util.Date;
import java.util.List;

@JsonIgnoreProperties(value = { "storageClasses", "parentItem" })
public class DriveItem {

	private int driveId;

	private String path;

	private Date modifiedDate;

	private long fileSize;

	private boolean directory;

	private StorageClass storageClass;

	private List<AccessLevel> accessLevels;

	private boolean isRestoring;

	private String restoreExpireDate;

	public DriveItem() {
	}

	public DriveItem(String path) {
		this.path = path;
	}

	public DriveItem(int driveId, String path) {
		this.driveId = driveId;
		this.path = path;
	}

	public int getDriveId() {
		return driveId;
	}

	public void setDriveId(int driveId) {
		this.driveId = driveId;
	}

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public Date getModifiedDate() {
		return modifiedDate;
	}

	public void setModifiedDate(Date modifiedDate) {
		this.modifiedDate = modifiedDate;
	}

	public long getFileSize() {
		return fileSize;
	}

	public void setFileSize(long fileSize) {
		this.fileSize = fileSize;
	}

	public boolean isRestoring() {
		return isRestoring;
	}

	public void setRestoring(Boolean restoring) {
		isRestoring = Boolean.TRUE.equals(restoring);
	}

	public boolean isDirectory() {
		return directory;
	}

	public void setDirectory(boolean directory) {
		this.directory = directory;
	}

	public StorageClass getStorageClass() {
		return storageClass;
	}

	public boolean isRestoreRequired() {
		return storageClass != null && storageClass.isRestoreRequired();
	}

	public void setStorageClass(StorageClass storageClass) {
		this.storageClass = storageClass;
	}

	public List<AccessLevel> getAccessLevels() {
		return this.accessLevels;
	}

	public void setAccessLevels(List<AccessLevel> accessLevels) {
		this.accessLevels = accessLevels;
	}

	public String getRestoreExpireDate() {
		return restoreExpireDate;
	}

	public void setRestoreExpireDate(String restoreExpireDate) {
		this.restoreExpireDate = restoreExpireDate;
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof DriveItem)) {
			return false;
		}

		DriveItem that = (DriveItem) obj;

		return (driveId == that.driveId) && path.equals(that.path);
	}

	@Override
	public int hashCode() {
		return 	new HashCodeBuilder()
				.append(driveId)
				.append(path).build();
	}

	@Override
	public String toString() {
		return "DriveItem [path=" + path + "]";
	}

	public boolean isFile() {
		return !directory;
	}

	public boolean isRoot() {
		return PathProcessor.isRoot(path);
	}

	public boolean isPlaceholderFile() {
		return path != null && getFileName().equals(FileUtil.PLACEHOLDER_FILE_NAME);
	}

	public boolean matchesPath(String s) {
		return PathProcessor.matchesPath(path, s);
	}

	public String getFileName() {
		return PathProcessor.getFileName(path);
	}

	public String getParentFolderPath() {
		return PathProcessor.getParentFolderPath(path);
	}

	public boolean isInsideFolder(String startPath) {
		String tempPath = PathProcessor.removeFirstSlash(path);
		String tempStartPath = PathProcessor.removeFirstSlash(startPath);

		return !tempPath.equals(startPath) && tempPath.startsWith(tempStartPath);
	}

	public boolean isEmpty() {
		return fileSize == 0;
	}

	@JsonIgnore
	public DriveItem getParentItem() {
		String parentFolderPath = getParentFolderPath();

		DriveItem parentItem = new DriveItem();
		parentItem.setDriveId(driveId);
		parentItem.setPath(parentFolderPath);
		parentItem.setDirectory(true);

		return parentItem;
	}

}
