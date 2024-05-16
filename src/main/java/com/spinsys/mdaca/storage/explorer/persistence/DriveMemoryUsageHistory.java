package com.spinsys.mdaca.storage.explorer.persistence;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EntityManager;
import javax.persistence.EntityTransaction;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.PrePersist;
import javax.persistence.Table;
import javax.persistence.TemporalType;
import javax.persistence.TypedQuery;

import org.hibernate.annotations.GenericGenerator;

@Entity
@Table(name="DRIVE_MEMORY_USAGE_HISTORY")
public class DriveMemoryUsageHistory {
	
	private static final Logger logger =
			Logger.getLogger("com.spinsys.mdaca.storage.explorer.persistence.DriveMemoryUsageHistory");
	

	private int usageId;
	private String path;
	private Long bytes;
	private Date created;

	@PrePersist
	protected void onCreate() {
		setCreated(new Date());
	}

	public DriveMemoryUsageHistory() {
	}

	public DriveMemoryUsageHistory(String path) {
		this.path = path;
	}

	public DriveMemoryUsageHistory(String path, Long numBytes) {
		this.path = path;
		this.bytes = numBytes;
	}

	@Column(name = "CREATED")
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

	@Column(name = "PATH")
	public String getPath() {
		return path;
	}
	public void setPath(String path) {
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

	private Drive drive;

    @ManyToOne
    @JoinColumn(name="DRIVE_ID", nullable=false)
	public Drive getDrive() {
		return drive;
	}

	public void setDrive(Drive drive) {
		this.drive = drive;
	}

	/**
	 * Retrieve drive usage historical records based on the input criteria.
	 * @param driveId the drive of interest
	 * @param startDate the earliest date for which a record is wanted
	 * @param endDate the latest date for which a record is wanted
	 * @param numDesired the number of records desired
	 * @return a list of drive usage records
	 */
	public static List<DriveMemoryUsageHistory> getDriveUsageHistory(
			int driveId, Date startDate, Date endDate,
			int numDesired, EntityManager manager)
	{
		Timestamp startStamp = new Timestamp(startDate.getTime());
		Timestamp endStamp = new Timestamp(endDate.getTime());
		TypedQuery<DriveMemoryUsageHistory> query = manager
				.createQuery(
						"SELECT DISTINCT du FROM DriveMemoryUsageHistory du "
								+ "WHERE (du.drive.driveId = :driveId) "
								+ "AND du.created BETWEEN :start AND :end "
								+ "ORDER BY du.created asc ",
						DriveMemoryUsageHistory.class)
				.setParameter("start", startStamp, TemporalType.TIMESTAMP)
				.setParameter("end", endStamp, TemporalType.TIMESTAMP)
				.setParameter("driveId", driveId);
		List<DriveMemoryUsageHistory> results = query.getResultList();
				
		
		// If we have more records than were asked for, try to create
		// a representative sampling
		if (results.size() > numDesired) {
			results = getSampling(numDesired, results);
		}
		return results;
	}


	static List<DriveMemoryUsageHistory> getSampling(int numDesired, List<DriveMemoryUsageHistory> results) {
		List<DriveMemoryUsageHistory> newResults = new ArrayList<>(numDesired);
		int resultsSize = results.size();

		// Build the results.  Because the most recent results
		// are probably more interesting than the older ones,
		// we may omit some from the beginning
		int multiplier = resultsSize/numDesired;
		
		// calculate a start index to ensure that we will get
		// the last element
		int index = (resultsSize - 1) 	// the last index
					  - ((numDesired - 1) * multiplier); 

		while (newResults.size() < numDesired  && index < resultsSize) {
			newResults.add(results.get(index));
			index += multiplier;
		}
		// Make sure to get the first element too.
		// We care more about it than some other old one.
		newResults.set(0, results.get(0));
		results = newResults;
		return results;
	}

	@Override
	public String toString() {
		return "DriveUsage [usageId=" + usageId + ", path=" + path + 
				", bytes=" + bytes + ", created=" + created +
				", drive=" + drive + "]";
	}

	/**
	 * Save the usage data to the database.
	 */
	public static void saveUsageHistory(Drive drive, Long bytesUsed, EntityManager manager) {
		EntityTransaction transaction = manager.getTransaction();
	
		try {
			DriveMemoryUsageHistory usage = new DriveMemoryUsageHistory();
			usage.setBytes((bytesUsed == null) ? 0L : bytesUsed);
			usage.setCreated(new Date(System.currentTimeMillis()));
			usage.setDrive(drive);
			usage.setPath("/");
	
			transaction = manager.getTransaction();
			transaction.begin();
			logger.info("Saving usage history data - " + usage);
	        manager.persist(usage);
			transaction.commit();
		} catch (Exception e) {
			try {
				logger.log(Level.WARNING, e.getMessage(), e);
	
				if (transaction != null) {
					transaction.rollback();
				}
			} catch (Exception e1) {
				logger.log(Level.WARNING, "Exception during rollback", e);
			}
		}
	}

}
