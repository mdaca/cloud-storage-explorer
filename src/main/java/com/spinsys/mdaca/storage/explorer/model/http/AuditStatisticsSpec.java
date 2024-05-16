package com.spinsys.mdaca.storage.explorer.model.http;

import java.util.Date;

/** All action/operations whose status match this will have
 *  statistics (quantities) reported, e.g., for "pending"
 *  operations that began over the last hour, the return
 *  value might indicate:
 *  copy - 5
 *  download - 8
 *  upload - 7
 *  etc.
 */
public class AuditStatisticsSpec {

	/** The start time from which statistics are desired. */
	Date fromDate;
	
	String status; // e.g., pending

	public Date getFromDate() {
		return fromDate;
	}

	public void setFromDate(Date fromDate) {
		this.fromDate = fromDate;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	@Override
	public String toString() {
		return "AuditStatisticsSpec [fromDate=" + fromDate + ", status=" + status + "]";
	}
	

}
