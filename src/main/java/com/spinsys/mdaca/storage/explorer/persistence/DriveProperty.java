package com.spinsys.mdaca.storage.explorer.persistence;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

import org.hibernate.annotations.GenericGenerator;

@Entity
@Table(name="DRIVE_PROPERTY")
public class DriveProperty {
	private int propertyId;
	private String propertyKey;
	private String propertyValue;
	

	public DriveProperty() {
		
	}

	public DriveProperty(String key) {
		this.propertyKey = key;
	}

	public DriveProperty(String key, String value) {
		this.propertyKey = key;
		this.propertyValue = value;
	}

	@Column(name = "PROPERTY_VALUE")
	public String getPropertyValue() {
		return propertyValue;
	}
	public void setPropertyValue(String propertyValue) {
		this.propertyValue = propertyValue;
	}

	@Column(name = "PROPERTY_KEY")
	public String getPropertyKey() {
		return propertyKey;
	}
	public void setPropertyKey(String propertyKey) {
		this.propertyKey = propertyKey;
	}

	@Id
	@GeneratedValue()
	 @GenericGenerator(name = "autoincrement", strategy = "identity")
	public int getPropertyId() {
		return propertyId;
	}
	public void setPropertyId(int propertyId) {
		this.propertyId = propertyId;
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

	@Override
	public String toString() {
		String driveId = (drive == null) ? null : "" + drive.getDriveId();
		return "DriveProperty [propertyKey=" + propertyKey + ", driveId=" + driveId + ", propertyValue=******]";
	}
}
