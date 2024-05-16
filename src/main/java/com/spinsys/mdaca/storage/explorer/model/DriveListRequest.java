package com.spinsys.mdaca.storage.explorer.model;

import java.util.List;

import com.spinsys.mdaca.storage.explorer.persistence.Drive;

public class DriveListRequest {

    private List<Drive> drives;

    public DriveListRequest() {
    }

    public List<Drive> getDrives() {
        return drives;
    }

    public void setDrives(List<Drive> drives) {
        this.drives = drives;
    }

}
