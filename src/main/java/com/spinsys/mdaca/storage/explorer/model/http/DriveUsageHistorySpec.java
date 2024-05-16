package com.spinsys.mdaca.storage.explorer.model.http;

import java.util.Date;

/**
 * This class is used to request the drive usage history for
 * a drive, i.e., the changes in how much disk space was used
 * over time.
 */
public class DriveUsageHistorySpec {

	/** A unique identifier for the drive. */
	protected int driveId;
	
	/** Request drive usage starting at this time represented as a long. */
	protected long startTime;

	/** Request drive usage ending at this time represented as a long. */
	protected long endTime;

	/** The desired maximum number of objects in the response. */
	protected int responseSize;

	public int getDriveId() {
		return driveId;
	}

	public void setDriveId(int driveId) {
		this.driveId = driveId;
	}

	public long getStartTime() {
		return startTime;
	}

	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}

	public long getEndTime() {
		return endTime;
	}

	public void setEndTime(Long endTime) {
		this.endTime = endTime;
	}

	public int getResponseSize() {
		return responseSize;
	}

	public void setResponseSize(int responseSize) {
		this.responseSize = responseSize;
	}

	public Date getStartDate() {
		return new Date(startTime);
	}

	public Date getEndDate() {
		return new Date(endTime);
	}

	@Override
	public String toString() {
		return "DriveUsageHistorySpec [driveId=" + driveId +
				", startDate=" + getStartDate() +
				", endDate=" + getEndDate() + "]";
	}


}
