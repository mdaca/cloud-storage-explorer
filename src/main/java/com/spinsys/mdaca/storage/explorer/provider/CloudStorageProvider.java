package com.spinsys.mdaca.storage.explorer.provider;

import com.spinsys.mdaca.storage.explorer.model.DriveItem;
import com.spinsys.mdaca.storage.explorer.model.DriveQuery;
import com.spinsys.mdaca.storage.explorer.model.StorageClass;
import com.spinsys.mdaca.storage.explorer.model.exception.ExplorerException;
import com.spinsys.mdaca.storage.explorer.persistence.Drive;

import java.io.IOException;
import java.util.List;

public abstract class CloudStorageProvider<T> extends AbstractStorageProvider<T> {

    /** Hive Properties (cloud only) */
    public static final String HIVE_HOST_NAME = "HiveHostName";
    public static final String HIVE_PORT = "HivePort";

    public abstract List<StorageClass> getStorageClasses();

    public abstract void updateStorageClass(Drive drive, String path, StorageClass storageClass) throws IOException;

    public abstract List<DriveItem> find(Drive drive, DriveQuery query) throws ExplorerException;

}
