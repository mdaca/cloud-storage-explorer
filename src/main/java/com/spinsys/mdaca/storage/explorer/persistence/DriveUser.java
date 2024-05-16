package com.spinsys.mdaca.storage.explorer.persistence;

import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import org.hibernate.annotations.GenericGenerator;

import java.util.List;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EntityManager;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import javax.persistence.TypedQuery;

@Entity
@Table(name = "DRIVE_USER")
public class DriveUser {

    private int userId;

    private String userName;

    private Drive drive;

    @Id
	@GeneratedValue()
    @GenericGenerator(name = "autoincrement", strategy = "identity")
    @Column(name = "USER_ID")
    public int getUserId() {
        return userId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }

    @Column(name = "USER_NAME")
    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    @ManyToOne
    @JoinColumn(name = "DRIVE_ID", nullable = false)
    public Drive getDrive() {
        return drive;
    }

    public void setDrive(Drive drive) {
        this.drive = drive;
    }

    @Override
    public String toString() {
        return "DriveUser [userId=" + userId + ", userName='" + userName + '\'' + ", drive=" + drive + ']';
    }

	public static List<Drive> getDrivesByUserName(String userName, EntityManager manager) {
		TypedQuery<Drive> query =
				manager.createQuery("select distinct du.drive from DriveUser du " +
									"where (du.userName = :userName)", Drive.class)
				.setParameter("userName", userName);
		List<Drive> result = query.getResultList();
		return result;
	}


}
