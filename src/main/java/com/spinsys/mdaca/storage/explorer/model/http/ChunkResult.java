package com.spinsys.mdaca.storage.explorer.model.http;

public class ChunkResult {
	private boolean uploaded;
	private String fileUid;
	public boolean isUploaded() {
		return uploaded;
	}
	public void setUploaded(boolean uploaded) {
		this.uploaded = uploaded;
	}
	public String getFileUid() {
		return fileUid;
	}
	public void setFileUid(String fileUid) {
		this.fileUid = fileUid;
	}
	public ChunkResult() {
	
	}

	public ChunkResult(boolean uploaded, String fileUid) {
		this.uploaded = uploaded;
		this.fileUid = fileUid;
	}
}
