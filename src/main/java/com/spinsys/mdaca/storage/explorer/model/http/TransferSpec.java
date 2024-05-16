package com.spinsys.mdaca.storage.explorer.model.http;

public class TransferSpec {
	private int sourceDriveId;
	private String sourcePath;
	private int destDriveId;
	private String destPath;
	private boolean removeSource = false;
	private String resolution;
	
	public int getSourceDriveId() {
		return sourceDriveId;
	}
	public void setSourceDriveId(int sourceDriveId) {
		this.sourceDriveId = sourceDriveId;
	}
	public String getSourcePath() {
		return sourcePath;
	}
	public void setSourcePath(String sourcePath) {
		this.sourcePath = sourcePath;
	}
	public int getDestDriveId() {
		return destDriveId;
	}
	public void setDestDriveId(int destDriveId) {
		this.destDriveId = destDriveId;
	}
	public String getDestPath() {
		return destPath;
	}
	public void setDestPath(String destPath) {
		this.destPath = destPath;
	}
	public boolean isRemoveSource() {
		return removeSource;
	}
	public void setRemoveSource(boolean removeSource) {
		this.removeSource = removeSource;
	}
	public String getResolution() {
		return resolution;
	}
	public void setResolution(String resolution) {
		this.resolution = resolution;
	}
	
	@Override
	public String toString() {
		return "TransferSpec [sourceDriveId=" + sourceDriveId + ", sourcePath=" + sourcePath + ", destDriveId="
				+ destDriveId + ", destPath=" + destPath + ", removeSource=" + removeSource + "]";
	}

}
