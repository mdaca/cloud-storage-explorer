package com.spinsys.mdaca.storage.explorer.model.enumeration;

public enum AccessLevel {
    None("N"),
    Read("R"),
    Create("C"),
    Modify("M"),
    Delete("D"),
    Restore("U"),
    Archive("A");

    public String value;

    AccessLevel(String value) {
        this.value = value;
    }

    public static AccessLevel fromString(String value) {
        for (AccessLevel a : AccessLevel.values()) {
            if (a.value.equalsIgnoreCase(value)) {
                return a;
            }
        }
        return AccessLevel.None;
    }

    /**
     * @param levelToCheck  the maximum role the user has
     * @param levelToAssert the role the user is being asserted for
     * @return true IFF the levelToCheck is greater than or equal to the levelToAssert
     */
    public static boolean isApplicableAccessLevel(AccessLevel levelToCheck, AccessLevel levelToAssert) {
        switch (levelToCheck) {
            case None:
            case Restore:
            case Archive:
                return levelToAssert.equals(levelToCheck);

            //intentionally continue in the switch to check all valid access levels
            case Delete:
                if (levelToAssert.equals(Delete)) return true;
            case Modify:
                if (levelToAssert.equals(Modify)) return true;
            case Create:
                if (levelToAssert.equals(Create)) return true;
            case Read:
                if (levelToAssert.equals(Read)) return true;
        }

        return false;
    }

}
