package com.spinsys.mdaca.storage.explorer.model.http;

public class AppInfoResponse {

	private String licensedBy;
	private String licenseType;
	private String licenseExpiration;
	private String versionNumber;
	
	public String getLicensedBy() {
		return licensedBy;
	}
	public void setLicensedBy(String licensedBy) {
		this.licensedBy = licensedBy;
	}
	public String getLicenseType() {
		return licenseType;
	}
	public void setLicenseType(String licenseType) {
		this.licenseType = licenseType;
	}
	public String getLicenseExpiration() {
		return licenseExpiration;
	}
	public void setLicenseExpiration(String licenseExpiration) {
		this.licenseExpiration = licenseExpiration;
	}
	public String getVersionNumber() {
		return versionNumber;
	}
	public void setVersionNumber(String versionNumber) {
		this.versionNumber = versionNumber;
	}
}
