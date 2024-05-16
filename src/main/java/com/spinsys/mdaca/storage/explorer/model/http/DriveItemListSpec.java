package com.spinsys.mdaca.storage.explorer.model.http;

import com.spinsys.mdaca.storage.explorer.model.DriveItem;

import java.util.ArrayList;
import java.util.List;

public class DriveItemListSpec {

    private List<DriveItem> driveItems = new ArrayList<>();

    public List<DriveItem> getDriveItems() {
        return driveItems;
    }

    public void setDriveItems(List<DriveItem> driveItems) {
        this.driveItems = driveItems;
    }

    public int getDriveId() {
        if (driveItems.size() == 0) {
            return 0;
        } else {
            return driveItems.stream().findFirst().get().getDriveId();
        }
    }

}
