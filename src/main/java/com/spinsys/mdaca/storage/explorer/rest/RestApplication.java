package com.spinsys.mdaca.storage.explorer.rest;

import java.util.HashSet;
import java.util.Set;

import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Application;

@ApplicationPath("/rest")
public class RestApplication extends Application {
	@Override
	public Set<Class<?>> getClasses() {
	    Set<Class<?>> classes = new HashSet<Class<?>>();
	    classes.add(DriveService.class);
	    classes.add(WorkspaceService.class);
	    classes.add(UserService.class);
	    classes.add(AdminService.class);
	    classes.add(MetricsService.class);
	    return classes;
	}
	}
