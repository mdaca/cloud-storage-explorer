package com.spinsys.mdaca.storage.explorer.milton;

import java.util.ArrayList;
import java.util.List;

import io.milton.annotations.ChildOf;
import io.milton.annotations.ChildrenOf;
import io.milton.annotations.ResourceController;
import io.milton.annotations.Users;

@ResourceController
public class DavUserController {

    private List<DavUser> users = new ArrayList<DavUser>();
    
    @ChildrenOf
    @Users // ties in with the @AccessControlList and @Authenticate methods below
    public List<DavUser> getUsers(DavUserController root) {
    	return users;
    }

    @ChildOf
    @Users
    public DavUser findUserByName(DavUserController root, String name) {
        return new DavUser(name);
    }
    
    public DavUserController() {
        users.add(new DavUser("admin"));
        users.add(new DavUser("user"));
    }
}
