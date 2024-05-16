package com.spinsys.mdaca.storage.explorer.persistence;

import org.hibernate.annotations.GenericGenerator;
import org.hibernate.annotations.NotFound;
import org.hibernate.annotations.NotFoundAction;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EntityManager;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.PrePersist;
import javax.persistence.PreUpdate;
import javax.persistence.Table;
import javax.persistence.TemporalType;
import javax.persistence.TypedQuery;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

@Entity
@Table(name = "ACTION_AUDIT")
public class ActionAudit {

	// Status codes
	public static final String ERROR = "E";
	public static final String PENDING = "P";
	public static final String SUCCESS = "S";
	public static final String UNAUTHORIZED = "U";

	private int actionAuditId;
	private String username;
	private String path;
	private Drive drive;
	private String action;
	private long bytesTransferred;
	private String status;
	private long durationMS;
	private String message;
	private String stackTrace;
	private String destPath;
	private int destDriveId;
	private Date created;
	private Date updated;
	private String ipAddress;
	private String oldStorageClass;
	private String newStorageClass;
	private boolean useMessage;
	private Integer percentComplete;
	private Long totalBytes;

	@PrePersist
	protected void onCreate() {
		setCreated(new Date());
	}

	@PreUpdate
	protected void onUpdate() {
		long startTime = created.getTime();

		setUpdated(new Date());
		setDurationMS(System.currentTimeMillis() - startTime);
	}

	@Column(name = "UPDATED")
	public Date getUpdated() {
		return updated;
	}

	public void setUpdated(Date updated) {
		this.updated = updated;
	}

	@Column(name = "CREATED")
	public Date getCreated() {
		return created;
	}

	public void setCreated(Date created) {
		this.created = created;
	}

	@Column(name = "ACTION_USERNAME")
	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = TableUtils.getSafeValue(username, 255);
	}

	@Column(name = "ACTION_PATH")
	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = TableUtils.getSafeValue(path, 255);
	}

	@ManyToOne
	@JoinColumn(name = "DRIVE_ID")
	@NotFound(action = NotFoundAction.IGNORE)
	public Drive getDrive() {
		return drive;
	}

	public void setDrive(Drive drive) {
		this.drive = drive;
	}

	@Column(name = "ACTION")
	public String getAction() {
		return action;
	}

	public void setAction(String action) {
		this.action = TableUtils.getSafeValue(action, 255);
	}

	@Column(name = "STATUS")
	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = TableUtils.getSafeValue(status, 255);
	}

	@Column(name = "BYTES_TRANSFERRED")
	public long getBytesTransferred() {
		return bytesTransferred;
	}

	public void setBytesTransferred(long bytesTransferred) {
		this.bytesTransferred = bytesTransferred;
	}

	@Column(name = "DURATION_MS")
	public long getDurationMS() {
		return durationMS;
	}

	public void setDurationMS(Long durationMS) {
		this.durationMS = durationMS;
	}

	@Column(name = "MESSAGE")
	public String getMessage() {
		return message;
	}

	public void setMessage(String msg) {
		this.message = TableUtils.getSafeValue(msg, 255);
	}

	@Column(name = "STACK_TRACE", length = 2048)
	public String getStackTrace() {
		return stackTrace;
	}

	public void setStackTrace(String trace) {
		if (trace != null) {
			this.stackTrace = TableUtils.getSafeValue(trace, 2048);
		}
	}

	@Id
	@GeneratedValue()
	@GenericGenerator(name = "autoincrement", strategy = "identity")
	@Column(name = "ACTION_AUDIT_ID")
	public int getActionAuditId() {
		return actionAuditId;
	}

	public void setActionAuditId(int actionAuditId) {
		this.actionAuditId = actionAuditId;
	}

	@Column(name = "DEST_PATH")
	public String getDestPath() {
		return destPath;
	}

	public void setDestPath(String destPath) {
		this.destPath = TableUtils.getSafeValue(destPath, 255);
	}

	@Column(name = "DEST_DRIVE_ID")
	public int getDestDriveId() {
		return destDriveId;
	}

	public void setDestDriveId(Integer destDriveId) {
		this.destDriveId = destDriveId;
	}

	@Column(name = "IP_ADDRESS")
	public String getIpAddress() {
		return ipAddress;
	}

	public void setIpAddress(String ipAddress) {
		this.ipAddress = TableUtils.getSafeValue(ipAddress, 255);
	}

	@Column(name = "newStorageClass")
	public String getNewStorageClass() {
		return newStorageClass;
	}

	public void setNewStorageClass(String newStorageClass) {
		this.newStorageClass = TableUtils.getSafeValue(newStorageClass, 255);
	}

	@Column(name = "oldStorageClass")
	public String getOldStorageClass() {
		return oldStorageClass;
	}

	public void setOldStorageClass(String oldStorageClass) {
		this.oldStorageClass = TableUtils.getSafeValue(oldStorageClass, 255);
	}

	@Column(name = "USE_MESSAGE", columnDefinition = "boolean default false")
	public boolean isUseMessage() {
		return useMessage;
	}

	public void setUseMessage(boolean useMessage) {
		this.useMessage = useMessage;
	}

	@Column(name = "PERCENT_COMPLETED")
	public Integer getPercentComplete() {
		return percentComplete;
	}

	public void setPercentComplete(Integer percentComplete) {
		this.percentComplete = percentComplete;
	}

	@Column(name = "TOTAL_BYTES")
	public Long getTotalBytes() {
		return totalBytes;
	}

	public void setTotalBytes(Long totalBytes) {
		this.totalBytes = totalBytes;
	}
	
	public static Map<String, Number> getActionStatistics(
			String status, Date from, EntityManager manager) {
		TypedQuery<Object[]> query =
				manager.createQuery("SELECT audit.action, count(audit.action) FROM ActionAudit audit" +
									" WHERE (audit.status = :status)" +
									" AND (audit.created > :from)" +
									" GROUP BY audit.action", Object[].class)
				.setParameter("status", status)
				.setParameter("from", from, TemporalType.TIMESTAMP);
		List<Object[]> results = query.getResultList();
		Map<String, Number> resultMap = new TreeMap<>();
		
		for (Object[] pair : results) {
			String operation = "" + pair[0];
			Number count = (pair[1] instanceof Number) ? (Number)pair[1] : 0;
			resultMap.put(operation , count);
		}
		return resultMap;
	}

	@Override
	public String toString() {
		return "ActionAudit [actionAuditId=" + actionAuditId + ", username=" + username + ", path=" + path + ", drive="
				+ drive + ", action=" + action + ", bytesTransferred=" + bytesTransferred + ", status=" + status
				+ ", durationMS=" + durationMS + ", message=" + message + ", stackTrace=" + stackTrace + ", destPath="
				+ destPath + ", destDriveId=" + destDriveId + ", created=" + created + ", updated=" + updated
				+ ", ipAddress=" + ipAddress + ", oldStorageClass=" + oldStorageClass + ", newStorageClass="
				+ newStorageClass + ", useMessage=" + useMessage + "]";
	}


}
