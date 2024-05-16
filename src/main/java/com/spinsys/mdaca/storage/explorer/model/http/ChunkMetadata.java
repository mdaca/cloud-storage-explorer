package com.spinsys.mdaca.storage.explorer.model.http;

public class ChunkMetadata {

    private String fileUid;
    private String fileName;
    private String contentType;
    private long chunkIndex;
    private long totalChunks;
    private long fileSize;

    public String getFileUid() {
        return fileUid;
    }

    public void setFileUid(String uploadUid) {
        this.fileUid = uploadUid;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getContentType() {
        return contentType;
    }

    public void setContentType(String contentType) {
        this.contentType = contentType;
    }

    public long getChunkIndex() {
        return chunkIndex;
    }

    public void setChunkIndex(long chunkIndex) {
        this.chunkIndex = chunkIndex;
    }

    public long getTotalChunks() {
        return totalChunks;
    }

    public void setTotalChunks(long totalChunks) {
        this.totalChunks = totalChunks;
    }

    public long getFileSize() {
        return fileSize;
    }

    public void setFileSize(long totalFileSize) {
        this.fileSize = totalFileSize;
    }

    public boolean isStart() {
    	return chunkIndex == 0;
	}

	public boolean isEnd() {
		return (totalChunks - 1) == chunkIndex;
	}

}
