package com.spinsys.mdaca.storage.explorer.persistence;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.spinsys.mdaca.storage.explorer.model.enumeration.AccessLevel;
import com.spinsys.mdaca.storage.explorer.rest.BaseService;
import org.hibernate.annotations.GenericGenerator;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import javax.persistence.Transient;

import java.security.Principal;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.spinsys.mdaca.storage.explorer.io.PathProcessor.GUI_SEP;
import static com.spinsys.mdaca.storage.explorer.model.enumeration.AccessLevel.isApplicableAccessLevel;

@Entity
@Table(name = "DRIVE_SECURITY_RULE")
public class DriveSecurityRule {

    private int ruleId;

    private String roleName;
    private String ruleText;
    private boolean exclude;
    private String accessLevel;
    private String users;

    @Column(name = "ROLE_NAME")
    public String getRoleName() {
        return roleName;
    }

    public void setRoleName(String roleName) {
        this.roleName = roleName;
    }

    @Column(name = "EXCLUDE")
    public boolean isExclude() {
        return exclude;
    }

    public void setExclude(boolean exclude) {
        this.exclude = exclude;
    }

    @Column(name = "RULE_TEXT")
    public String getRuleText() {
        return ruleText;
    }

    public void setRuleText(String ruleText) {
        this.ruleText = ruleText;
    }

    @Id
	@GeneratedValue()
    @GenericGenerator(name = "autoincrement", strategy = "identity")
    public int getRuleId() {
        return ruleId;
    }

    public void setRuleId(int ruleId) {
        this.ruleId = ruleId;
    }


	private Drive drive;

    @ManyToOne
    @JoinColumn(name="DRIVE_ID", nullable=false)
	public Drive getDrive() {
		return drive;
	}

	public void setDrive(Drive drive) {
		this.drive = drive;
	}

    public void setAccessLevel(String accessLevel) {
        this.accessLevel = accessLevel;
    }

    @Column(name = "ACCESS_LEVEL", length = 1)
    public String getAccessLevel() {
        return accessLevel;
    }

    @Column(name = "USERS")
    public String getUsers() {
        return users;
    }

    public void setUsers(String users) {
        this.users = users;
    }

    @Override
    public String toString() {
        String driveId = (drive == null) ? null : "" + drive.getDriveId();
        return "DriveSecurityRule [ruleId=" + ruleId + ", driveId=" + driveId + ", roleName=" + roleName + ", ruleText="
                + ruleText + ", exclude=" + exclude + ", accessLevel=" + accessLevel + "]";
    }

    public Set<String> usersAsSet() {
        if (users == null || users.isEmpty()) {
            return new HashSet<>();
        }
        if (!users.contains(";")) {
            return new HashSet<>(Collections.singleton(users));
        } else {
            List<String> usersList = Arrays.asList(users.split(";"));

            return new HashSet<>(usersList);
        }
    }

    /**
     * Determines whether the supplied path may be seen based on the rule text.
     *
     * @param path the string to test against the regex ruleText
     * @return true if the path matches the regex ruleText
     * example:
     *   ruleText: folder/subfolder/.*
     *   path: folder/subfolder/file.txt
     *   returns true*/
    public boolean passesRule(String path) {
    	boolean matches = path.matches(ruleText);
        boolean passes = exclude ? !matches : matches;
        return passes;
    }

    /**
     * Tests to see whether the supplied path is that of an
     * ancestor directory within the rule text.
     * @param path the string to test against the regex ruleText
     * @return true if the ruleText starts with the path AND the path is a folder
     * <p>
     * ruleText: folder/subfolder/.*
     *   path: folder/
     *   returns true
     * ruleText: folder/subfolder/.*
     *   path: fol
     *   returns false
     */
	public boolean matchesAncestorDirectory(String path) {
		return ruleText.startsWith(path) &&
                // check that we are matching a folder, not any old string
                path.endsWith(GUI_SEP);
	}

    /**
     * @param accessLevel access level to be checked
     * @return true iff the current user and required access level apply to this DriveSecurityRole
     */
    public boolean isApplicableToEvaluate(AccessLevel accessLevel) {
        boolean isApplicableAccessLevel = isApplicableAccessLevel(AccessLevel.fromString(this.accessLevel), accessLevel);

        return isUserApplicable() && isApplicableAccessLevel;
    }

    /**
     * @param accessLevel access level to be checked
     * @return true iff the current user and required access level apply to this DriveSecurityRole
     */
    public boolean isApplicableToEvaluate(AccessLevel accessLevel, List<String> roles, String username) {
        boolean isApplicableAccessLevel = isApplicableAccessLevel(AccessLevel.fromString(this.accessLevel), accessLevel);

        return isUserApplicable(roles, username) && isApplicableAccessLevel;
    }

    /**
     * @return true iff the user is included in the "users" field list, OR the  user has permissions to the applicable role
     */
    @Transient
    private boolean isUserApplicable() {

        Set<String> applicableUsers = usersAsSet();
        boolean roleNameEmpty = roleName == null || roleName.isEmpty();

        //if the user role is set, the current user must be in that role to be considered applicable
        boolean isApplicableToUser = applicableUsers.isEmpty() ||
                applicableUsers.contains(BaseService.getCurrentUsername());

        //if the role name is set, check if the current user is in that role
        boolean isApplicableToRole = roleNameEmpty ||
        		BaseService.getHttpServletRequest().isUserInRole(roleName);

        return isApplicableToUser && isApplicableToRole;
    }

    /**
     * @return true iff the user is included in the "users" field list, OR the  user has permissions to the applicable role
     */
    @Transient
    private boolean isUserApplicable(List<String> roles, String username) {

        Set<String> applicableUsers = usersAsSet();
        boolean roleNameEmpty = roleName == null || roleName.isEmpty();

        //if the user role is set, the current user must be in that role to be considered applicable
        boolean isApplicableToUser = applicableUsers.isEmpty() ||
                applicableUsers.contains(username);

        //if the role name is set, check if the current user is in that role
        boolean isApplicableToRole = roleNameEmpty ||
                BaseService.isInGroup(roleName, roles);

        return isApplicableToUser && isApplicableToRole;
    }

}
