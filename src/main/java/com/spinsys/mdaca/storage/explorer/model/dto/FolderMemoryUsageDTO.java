package com.spinsys.mdaca.storage.explorer.model.dto;

import java.util.Date;

/**
 * This class is a simple container for transmitting
 * folder memory usage information.
 */
public class FolderMemoryUsageDTO {
	
	private String path;
	
	/** The number of bytes being used. */
	private Long bytes;

	/** When this usage information was generated. */
	private Date created;

	public Date getCreated() {
		return created;
	}
	public void setCreated(Date created) {
		this.created = created;
	}

	public Long getBytes() {
		return bytes;
	}
	public void setBytes(Long bytes) {
		this.bytes = bytes;
	}
	public String getPath() {
		return path;
	}
	public void setPath(String path) {
		this.path = path;
	}

	@Override
	public String toString() {
		return "FolderMemoryUsageDTO [path=" + path +
				", bytes=" + bytes + ", created=" + created + "]";
	}

}
