package com.spinsys.mdaca.storage.explorer.model.dto;

import java.util.Date;

/**
 * This class is a simple container for transmitting
 * disk usage information.
 */
public class DriveMemoryUsageDTO {
	/** The number of bytes being used. */
	private Long bytes;

	/** The path for to the folder; uses "/" if it is a drive */
	private String path = "/";

	/** When this usage information was generated. */
	private Date created;

	public DriveMemoryUsageDTO() {
	}

	public DriveMemoryUsageDTO(Long bytes, String path, Date created) {
		this.bytes = bytes;
		this.path = path;
		this.created = created;
	}

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
		return "DriveUsageDTO [bytes=" + bytes + ", created=" + created + ", path=" + path + "]";
	}

}
