package com.spinsys.mdaca.storage.explorer.persistence;

import com.spinsys.mdaca.storage.explorer.model.StorageClass;
import com.spinsys.mdaca.storage.explorer.model.enumeration.DriveType;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.hibernate.annotations.GenericGenerator;
import org.hibernate.annotations.LazyCollection;
import org.hibernate.annotations.LazyCollectionOption;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EntityManager;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.PrePersist;
import javax.persistence.PreUpdate;
import javax.persistence.Table;
import javax.persistence.Transient;
import javax.persistence.TypedQuery;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import static com.spinsys.mdaca.storage.explorer.model.enumeration.AccessLevel.Read;
import static com.spinsys.mdaca.storage.explorer.rest.BaseService.getCurrentUsername;

@Entity
@Table(name = "DRIVE")
public class Drive implements Cloneable {

	/** Azure Blob */
	public static String DRIVE_TYPE_BLOB = "Blob";
	
	/** Google Cloud Storage */
	public static String DRIVE_TYPE_GCS = "GCS";
	
	/** Amazon Web Services (AWS) Simple Storage Service (S3) */
	public static String DRIVE_TYPE_S3 = "S3";
	
	/** Server Message Block */
	public static String DRIVE_TYPE_SMB = "SMB";
	
	public static String DRIVE_TYPE_WINDOWS = "Windows";
	
	public static int UNKNOWN_DRIVE_ID = -1;

	private int driveId;

	private String displayName;

	private DriveType driveType;

	private Date created;
	private Date updated;

	private String createdBy;
	private String updatedBy;

	private List<DriveSecurityRule> securityRules = new ArrayList<>();

	private List<StorageClass> storageClasses = new ArrayList<>();

	private boolean requiresDaysToExpire;

	private List<DriveUser> users = new ArrayList<>();

	private List<DriveProperty> providerProperties = new ArrayList<>();

	public Drive() {
	}

	public Drive(int driveId) {
		this.driveId = driveId;
	}

	@PrePersist
	protected void onCreate() {
		setCreated(new Date());
		setCreatedBy(getCurrentUsername());
	}

	@PreUpdate
	protected void onUpdate() {
		setUpdated(new Date());
		setUpdatedBy(getCurrentUsername());
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

	@Id
	@GeneratedValue()
	@GenericGenerator(name = "autoincrement", strategy = "identity")
	@Column(name = "DRIVE_ID")
	public int getDriveId() {
		return driveId;
	}

	public void setDriveId(int driveId) {
		this.driveId = driveId;
	}

	@Column(name = "DISPLAY_NAME")
	public String getDisplayName() {
		return displayName;
	}

	public void setDisplayName(String displayName) {
		this.displayName = displayName;
	}

	@Column(name = "DRIVE_TYPE")
	@Enumerated(value= EnumType.STRING)
	public DriveType getDriveType() {
		return driveType;
	}

	public void setDriveType(DriveType driveType) {
		this.driveType = driveType;
	}

	@LazyCollection(LazyCollectionOption.FALSE)
	@OneToMany(mappedBy = "drive", cascade = { CascadeType.ALL, CascadeType.MERGE,
			CascadeType.PERSIST }, orphanRemoval = true)
	public List<DriveSecurityRule> getSecurityRules() {
		return this.securityRules;
	}

	public void setSecurityRules(List<DriveSecurityRule> rules) {
		this.securityRules = rules;
	}

	@LazyCollection(LazyCollectionOption.FALSE)
	@OneToMany(mappedBy = "drive", cascade = { CascadeType.ALL, CascadeType.MERGE,
			CascadeType.PERSIST }, orphanRemoval = true)
	public List<DriveUser> getUsers() {
		return this.users;
	}

	public void setUsers(List<DriveUser> users) {
		this.users = users;
	}

	@LazyCollection(LazyCollectionOption.FALSE)
	@OneToMany(mappedBy = "drive", cascade = { CascadeType.ALL, CascadeType.MERGE,
			CascadeType.PERSIST }, orphanRemoval = true)
	public List<DriveProperty> getProviderProperties() {
		return this.providerProperties;
	}

	public void setProviderProperties(List<DriveProperty> props) {
		this.providerProperties = props;
	}

	@Column(name = "UPDATED_BY")
	public String getUpdatedBy() {
		return updatedBy;
	}

	public void setUpdatedBy(String updatedBy) {
		this.updatedBy = updatedBy;
	}

	@Column(name = "CREATED_BY")
	public String getCreatedBy() {
		return createdBy;
	}

	public void setCreatedBy(String createdBy) {
		this.createdBy = createdBy;
	}

	@Transient
	public List<StorageClass> getStorageClasses() {
		return storageClasses;
	}

	public void setStorageClasses(List<StorageClass> storageClasses) {
		this.storageClasses = storageClasses;
	}

	@Transient
	public boolean getRequiresDaysToExpire() {
		return requiresDaysToExpire;
	}

	public void setRequiresDaysToExpire(boolean requiresDaysToExpire) {
		this.requiresDaysToExpire = requiresDaysToExpire;
	}

	@Override
	public String toString() {
		return "Drive [driveId=" + driveId + ", displayName=" + displayName + ", driveType=" + driveType + "]";
	}

	public Object clone() throws CloneNotSupportedException {
		return super.clone();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}

		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		Drive drive = (Drive) o;

		return driveId == drive.driveId;
	}

	@Override
	public int hashCode() {
		return new HashCodeBuilder(17, 37)
				.append(driveId)
				.toHashCode();
	}

	public String getPropertyValue(String key) {
		for (DriveProperty prop : providerProperties) {
			if (prop.getPropertyKey().equals(key)) {
				return prop.getPropertyValue();
			}
		}
		return null;
	}

	public void addPropertyValue(String key, String value) {
		DriveProperty prop = new DriveProperty(key, value);
		prop.setDrive(this);
		this.providerProperties.add(prop);
	}

	public void voidMappedClasses() {
		if (providerProperties != null) {
			providerProperties.clear();
		}
		if (users != null) {
			users.clear();
		}
		if (securityRules != null) {
			securityRules.clear();
		}
	}

	public void voidMappedClassesAdmin() {
		if (storageClasses != null) {
			providerProperties.forEach(property -> property.setDrive(null));
		}
		if (users != null) {
			users.forEach(user -> user.setDrive(null));
		}
		if (securityRules != null) {
			securityRules.forEach(securityRule -> securityRule.setDrive(null));
		}
	}


	/**
	 * @return true iff there are ANY included read rules to this drive
	 */
	public boolean hasReadAccess() {
		return securityRules.stream()
				.filter(rule -> rule.isApplicableToEvaluate(Read))
				.anyMatch(rule -> !rule.isExclude());
	}

	/**
	 * returns the complete list of provider properties
	 * if a key-value pair is missing from the database, this method will add those missing key values to a new, complete list
	 * streams over the validProperties list to maintain the validProperties order
	 */
	public void populateProviderProperties(List<String> validProperties) {
		providerProperties = validProperties
				.stream()
				.map(key -> new DriveProperty(key, getPropertyValue(key)))
				.collect(Collectors.toList());
	}

	public static List<Drive> getDrivesByDriveId(int driveId, EntityManager manager) {
		List<Drive> drives =
				manager.createQuery("from Drive d " +
									"where (d.driveId = :driveId)", Drive.class)
				.setParameter("driveId", driveId)
				.getResultList();
		return drives;
	}


}
