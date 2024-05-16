package com.spinsys.mdaca.storage.explorer.model.http;

public class DownloadSpec extends FileLocationSpec {

	private int topLines;
	
	private int bottomLines;
	
	@Override
	public String toString() {
		return "DriveQuery [driveId=" + driveId + ", path=" + path + "]";
	}

	public int getTopLines() {
		return topLines;
	}

	public void setTopLines(int topLines) {
		this.topLines = topLines;
	}

	public int getBottomLines() {
		return bottomLines;
	}

	public void setBottomLines(int bottomLines) {
		this.bottomLines = bottomLines;
	}

	public boolean isPreview() {
		return topLines > 0 || bottomLines > 0;
	}

		
}
