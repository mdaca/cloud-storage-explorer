package com.spinsys.mdaca.storage.explorer.persistence;

import java.util.Date;
import java.util.List;
import java.util.logging.Logger;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EntityManager;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import javax.persistence.Temporal;
import javax.persistence.TemporalType;
import javax.persistence.TypedQuery;

import org.hibernate.annotations.GenericGenerator;

@Entity
@Table(name="MEMORY_USAGE")
public class MemoryUsage {
	
	/** We insert a record with this special value as a path to show
	 * that data collection is complete.
	 */
	public static final String COMPLETED = "///COMPLETED///";

	private static final Logger logger =
			Logger.getLogger("com.spinsys.mdaca.storage.explorer.persistence.MemoryUsage");
	

	private int usageId;
	private String path;
	private Long bytes;
	private Date created;
	private Drive drive;

	public MemoryUsage() {
	}

	public MemoryUsage(String path) {
		this.path = path;
	}

	public MemoryUsage(String path, Long numBytes) {
		this.path = path;
		this.bytes = numBytes;
	}

	public MemoryUsage(int driveId, Long numBytes, String path) {
		this.drive = new Drive(driveId);
		this.bytes = numBytes;
		this.path = path;
	}

	@Id
	@GeneratedValue()
	 @GenericGenerator(name = "autoincrement", strategy = "identity")
	public int getUsageId() {
		return usageId;
	}

	public void setUsageId(int usageId) {
		this.usageId = usageId;
	}

	@Column(name = "CREATED")
	@Temporal(TemporalType.TIMESTAMP)
	public Date getCreated() {
		return created;
	}

	public void setCreated(Date created) {
		this.created = created;
	}

	@Column(name = "BYTES")
	public Long getBytes() {
		return bytes;
	}
	public void setBytes(Long bytes) {
		this.bytes = bytes;
	}

	@Column(name = "PATH", length = 2048)
	public String getPath() {
		return path;
	}
	public void setPath(String path) {
	    // TODO why 255?
        this.path = TableUtils.getSafeValue(path, 255);
	}

    @ManyToOne
    @JoinColumn(name="DRIVE_ID", nullable=false)
	public Drive getDrive() {
		return drive;
	}

	public void setDrive(Drive drive) {
		this.drive = drive;
	}

	/**
	 * Retrieve folder usage records based on the input criteria.
	 * One usage record will be returned for each subfolder.
	 * Each usage record will indicate the total disk usage for that subfolder
	 * and all of its contents, recursively.
	 * @param driveId the drive of interest
	 * @param startDate the earliest date for which a record is wanted
	 * @param path the folder for which usage information is desired
	 * @return a list of drive usage records
	 */
	public static Long getFolderUsage(
			int driveId, String path, EntityManager manager)
	{
		Long result = null;
		Date lastCompleted = getLastCompletedTime(driveId, manager);
		
		if (lastCompleted != null ) {
			// get things newer than 1 minute before the data was recorded
			// TODO get the date/time parameter types correct, so we
			// don't have to subtract a minute and can use equals instead
			Date when = new Date(lastCompleted.getTime() - 60_000);
			TypedQuery<Long> query = manager
					.createQuery(
							"SELECT SUM(usage.bytes) FROM MemoryUsage usage"
									+ " WHERE (usage.drive.driveId = :driveId)"
									+ " AND (usage.path LIKE '" + path + "%')"
//									+ " AND (usage.created = :created)",
									+ " AND (usage.created > :created)",
							Long.class)
					.setParameter("driveId", driveId)
//					.setParameter("created", lastCompleted, TemporalType.TIMESTAMP);
					.setParameter("created", when, TemporalType.TIMESTAMP);
			result = query.getSingleResult();
		}
		else {
			logger.warning("No folder data usage available for drive " + driveId);
		}
		return result;
	}

	public static List<MemoryUsage> getBiggestFiles(
			int driveId, String path, int maxResults, EntityManager manager)
	{
		List<MemoryUsage> result = null;
		Date lastCompleted = getLastCompletedTime(driveId, manager);
		
		if (lastCompleted != null ) {
			// get things newer than 1 minute before the data was recorded
			// TODO get the date/time parameter types correct, so we
			// don't have to subtract a minute and can use equals instead
			Date when = new Date(lastCompleted.getTime() - 60_000);
			TypedQuery<MemoryUsage> query = manager
					.createQuery(
							"SELECT usage FROM MemoryUsage usage"
									+ " WHERE (usage.drive.driveId = :driveId)"
									+ " AND (usage.path LIKE '" + path + "%')"
//									+ " AND (usage.created = :created)",
									+ " AND (usage.created > :created)"
									+ " ORDER BY usage.bytes DESC",
									MemoryUsage.class)
					.setParameter("driveId", driveId)
//					.setParameter("created", lastCompleted, TemporalType.TIMESTAMP);
					.setParameter("created", when, TemporalType.TIMESTAMP)
					.setMaxResults(maxResults);
			result = query.getResultList();
		}
		else {
			logger.warning("No folder data usage available for drive " + driveId);
		}
		return result;
	}

	/**
	 * Delete old folder usage records based on the input criteria.
	 * @param driveId the drive of interest
	 * @param startDate the earliest date for which a record is wanted
	 * @param path the folder for which usage information is desired
	 * @return a list of drive usage records
	 */
	public static int deleteOldFolderUsage(
			int driveId, EntityManager manager)
	{
		int rowsDeleted = 0;
		Date lastCompleted = getLastCompletedTime(driveId, manager);
		
		if (lastCompleted != null ) {
			// delete records older than 1 minute before the data was recorded
			// TODO get the date/time parameter types correct, so we
			// don't have to subtract a minute and can use equals instead
			Date when = new Date(lastCompleted.getTime() - 60_000);
			javax.persistence.Query query = manager
					.createQuery(
							"DELETE FROM MemoryUsage usage"
									+ " WHERE (usage.drive.driveId = :driveId)"
									+ " AND (usage.created < :created)")
					.setParameter("driveId", driveId)
//					.setParameter("created", lastCompleted, TemporalType.TIMESTAMP);
					.setParameter("created", when, TemporalType.TIMESTAMP);
			rowsDeleted = query.executeUpdate();
		}
		else {
			logger.warning("No folder data usage available for drive " + driveId);
		}
		return rowsDeleted;
	}

	public static Date getLastCompletedTime(
			int driveId, EntityManager manager)
	{
		TypedQuery<Date> query = manager
				.createQuery(
						"SELECT usage.created FROM MemoryUsage usage"
								+ " WHERE (usage.drive.driveId = :driveId) "
								+ " AND (usage.path='" + COMPLETED + "')"
								+ " ORDER BY usage.created DESC",
						Date.class)
				.setParameter("driveId", driveId)
				.setMaxResults(1);
		List<Date> resultList = query.getResultList();
		Date result = (resultList != null && resultList.size() > 0)
						? resultList.get(0) : null;
		return result;
	}


	@Override
	public String toString() {
		return "MemoryUsage [usageId=" + usageId +
				", path=" + path + ", bytes=" + bytes +
				", created=" + created + ", drive=" + drive + "]";
	}

}
