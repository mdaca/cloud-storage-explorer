package com.spinsys.mdaca.storage.explorer.model.http;

import com.spinsys.mdaca.storage.explorer.model.StorageClass;

public class StorageClassSpec extends DriveItemListSpec {

    private StorageClass newStorageClass;

     public StorageClass getNewStorageClass() {
        return newStorageClass;
    }

    public void setNewStorageClass(StorageClass newStorageClass) {
        this.newStorageClass = newStorageClass;
    }

}
