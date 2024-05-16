package com.spinsys.mdaca.storage.explorer.persistence;

import org.hibernate.annotations.GenericGenerator;

import java.io.Serializable;
import java.util.Date;

import javax.persistence.*;

@Entity
@Table(name="WORKSPACE_CONFIG")
public class WorkspaceConfig {

	private int workspaceId;
	private int leftDriveId;
	private String leftPath;
	private int rightDriveId;
	private String rightPath;
	private String workspaceName;
	private String workspaceUser;
	  private Date created;
	  private Date updated;
	  private String rightSortField;
	  private boolean rightSortAsc;
	  private String rightSearchText;
	  private String leftSortField;
	  private boolean leftSortAsc;
	  private String leftSearchText;
	
	  @PrePersist
	  protected void onCreate() {
	    setCreated(new Date());
	  }
	
	  @PreUpdate
	  protected void onUpdate() {
	    setUpdated(new Date());
	  }

		@Column(name = "UPDATED")
		public Date getUpdated() {
			return updated;
		}

		public void setUpdated(Date updated) {
			this.updated = updated;
		}

		@Column(name = "CREATED")
		public Date getCreated() {
			return created;
		}

		public void setCreated(Date created) {
			this.created = created;
		}
		
	@Id
	@GeneratedValue()
	 @GenericGenerator(name = "autoincrement", strategy = "identity")
	@Column(name = "WORKSPACE_ID")
	public int getWorkspaceId() {
		return workspaceId;
	}
	public void setWorkspaceId(int workspaceId) {
		this.workspaceId = workspaceId;
	}

	@Column(name = "LEFT_DRIVE_ID")
	public int getLeftDriveId() {
		return leftDriveId;
	}
	public void setLeftDriveId(int leftDriveId) {
		this.leftDriveId = leftDriveId;
	}

	@Column(name = "LEFT_PATH")
	public String getLeftPath() {
		return leftPath;
	}
	public void setLeftPath(String leftPath) {
		this.leftPath = leftPath;
	}

	@Column(name = "RIGHT_DRIVE_ID")
	public int getRightDriveId() {
		return rightDriveId;
	}
	public void setRightDriveId(int rightDriveId) {
		this.rightDriveId = rightDriveId;
	}

	@Column(name = "RIGHT_PATH")
	public String getRightPath() {
		return rightPath;
	}
	public void setRightPath(String rightPath) {
		this.rightPath = rightPath;
	}
	
	@Column(name = "WORKSPACE_NAME")
	public String getWorkspaceName() {
		return workspaceName;
	}
	public void setWorkspaceName(String workspaceName) {
		this.workspaceName = workspaceName;
	}

	@Column(name = "WORKSPACE_USERNAME")
	public String getWorkspaceUser() {
		return workspaceUser;
	}
	public void setWorkspaceUser(String username) {
		this.workspaceUser = username;
	}


	@Column(name = "RIGHT_SORT_FIELD")
	public String getRightSortField() {
		return rightSortField;
	}

	public void setRightSortField(String rightSortField) {
		this.rightSortField = rightSortField;
	}

	@Column(name = "RIGHT_SORT_ASC")
	public boolean isRightSortAsc() {
		return rightSortAsc;
	}

	public void setRightSortAsc(boolean rightSortAsc) {
		this.rightSortAsc = rightSortAsc;
	}

	@Column(name = "LEFT_SORT_FIELD")
	public String getLeftSortField() {
		return leftSortField;
	}

	public void setLeftSortField(String leftSortField) {
		this.leftSortField = leftSortField;
	}

	@Column(name = "RIGHT_SEARCH_TEXT")
	public String getRightSearchText() {
		return rightSearchText;
	}

	public void setRightSearchText(String rightSearchText) {
		this.rightSearchText = rightSearchText;
	}

	@Column(name = "LEFT_SORT_ASC")
	public boolean isLeftSortAsc() {
		return leftSortAsc;
	}

	public void setLeftSortAsc(boolean leftSortAsc) {
		this.leftSortAsc = leftSortAsc;
	}

	@Column(name = "LEFT_SEARCH_TEXT")
	public String getLeftSearchText() {
		return leftSearchText;
	}

	public void setLeftSearchText(String leftSearchText) {
		this.leftSearchText = leftSearchText;
	}
}
