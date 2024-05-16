package com.spinsys.mdaca.storage.explorer.model.enumeration;

import com.spinsys.mdaca.storage.explorer.persistence.Drive;

public enum DriveType {

	Blob,
    GCS,
    S3,
    SMB,
	Windows;

	public static String toDatabaseDriveType(DriveType uiDrive) {
		String result = null;

		switch (uiDrive) {
		case Blob:
			result = Drive.DRIVE_TYPE_BLOB;
			break;
		case GCS:
			result = Drive.DRIVE_TYPE_GCS;
			break;
		case S3:
			result = Drive.DRIVE_TYPE_S3;
			break;
		case SMB:
			result = Drive.DRIVE_TYPE_SMB;
			break;
		case Windows:
			result = Drive.DRIVE_TYPE_WINDOWS;
			break;
		default:
			break;
		}
		return result;
	}
}
