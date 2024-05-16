package com.spinsys.mdaca.storage.explorer.model.http;

public class ExternalTableSpec extends FileLocationSpec {

    private String databaseName;
    private String tableName;

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public String toString() {
        return "ExternalTableSpec [sourceDriveId=" + driveId +
                ", sourcePath=" + path + ", databaseName="
                + databaseName + ", tableName=" + tableName + "]";
    }

}
