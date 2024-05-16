package com.spinsys.mdaca.storage.explorer.provider;

import com.spinsys.mdaca.storage.explorer.model.enumeration.DriveType;
import com.spinsys.mdaca.storage.explorer.model.exception.ExplorerException;

import net.sourceforge.spnego.SpnegoPrincipal;

import javax.servlet.http.HttpServletRequest;

public class StorageProviderFactory {

	public static StorageProvider getProvider(DriveType driveType) throws ExplorerException {
		switch (driveType) {
			case Blob:
				return new AzureBlobStorageProvider();
			case GCS:
				return new GoogleCloudStorageProvider();
			case S3:
				return new AWSS3StorageProvider();
			case SMB:
				return new SMBStorageProvider();
			case Windows:
				return new WindowsStorageProvider();
		}

		throw new ExplorerException("Storage provider " + driveType + " not implemented");
	}

	public static StorageProvider getProvider(DriveType driveType,
											SpnegoPrincipal principal) throws ExplorerException {
		switch (driveType) {
			case Blob:
				return new AzureBlobStorageProvider();
			case GCS:
				return new GoogleCloudStorageProvider();
			case S3:
				return new AWSS3StorageProvider();
			case SMB:
				return new SMBStorageProvider(principal);
			case Windows:
				return new WindowsStorageProvider();
		}

		throw new ExplorerException("Storage provider " + driveType + " not implemented");
	}

	public static StorageProvider getProvider(DriveType driveType,
											HttpServletRequest request) throws ExplorerException {
		switch (driveType) {
			case Blob:
				return new AzureBlobStorageProvider();
			case GCS:
				return new GoogleCloudStorageProvider();
			case S3:
				return new AWSS3StorageProvider();
			case SMB:
				return new SMBStorageProvider(request);
			case Windows:
				return new WindowsStorageProvider();
		}

		throw new ExplorerException("Storage provider " + driveType + " not implemented");
	}
}
