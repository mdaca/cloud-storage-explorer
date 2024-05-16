package com.spinsys.mdaca.storage.explorer.model;

public class StorageClass {

	private String className;
	private boolean restoreRequired;
	private String displayText;
	private boolean isDefault;
	
	public StorageClass() {
		
	}
	
	public StorageClass(String className, String displayText, boolean restoreRequired) {
		this(className, displayText, restoreRequired, false);
	}
	
	public StorageClass(String className, String displayText, boolean restoreRequired, boolean isDefault) {
		this.className = className;
		this.displayText = displayText;
		this.restoreRequired = restoreRequired;
		this.isDefault = isDefault;
	}

	public boolean isRestoreRequired() {
		return restoreRequired;
	}
	public void setRestoreRequired(boolean restoreRequired) {
		this.restoreRequired = restoreRequired;
	}
	public String getClassName() {
		return className;
	}
	public void setClassName(String className) {
		this.className = className;
	}

	public String getDisplayText() {
		return displayText;
	}

	public void setDisplayText(String displayText) {
		this.displayText = displayText;
	}

	@Override
	public String toString() {
		return displayText;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;

		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		StorageClass that = (StorageClass) o;

		return className.equals(that.className);
	}

	public boolean isDefault() {
		return isDefault;
	}

	public void setDefault(boolean aDefault) {
		isDefault = aDefault;
	}

}
