package com.spinsys.mdaca.storage.explorer.milton;

import java.util.HashMap;

import io.milton.http.HttpManager;
import io.milton.http.Request;
import io.milton.http.UrlAdapterImpl;
import io.milton.http.fs.NullSecurityManager;
import io.milton.servlet.DefaultMiltonConfigurator;

public class MiltonConfig extends DefaultMiltonConfigurator {
	
    protected void build() {
        //builder.setSecurityManager(new NullSecurityManager());
        //.builder..setContextPath("/storage-explorer/rest/dav/");
        builder.setEnableOptionsAuth(true);
        builder.setEnableBasicAuth(true);
        //builder.setRootDir(null);
        
        builder.setUrlAdapter(new UrlAdapterIml());
        super.build();
    }
    
    
    public class UrlAdapterIml extends UrlAdapterImpl {
    	
    	@Override
		public
    	String getUrl(Request request) {
    		String s = HttpManager.decodeUrl(request.getAbsolutePath());
    		return s.replaceAll("/storage-explorer/rest/dav", "");
    	}
    }
    
}
