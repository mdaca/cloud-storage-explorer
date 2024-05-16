package com.spinsys.mdaca.storage.explorer.provider;

import java.io.IOException;

import com.spinsys.mdaca.storage.explorer.persistence.Drive;

public abstract class RestorableCloudStorageProvider<T> extends CloudStorageProvider<T> {

    public abstract void restore(Drive drive, String path, int daysExpiration) throws IOException;
    public abstract boolean requiresDaysToExpire();

}
