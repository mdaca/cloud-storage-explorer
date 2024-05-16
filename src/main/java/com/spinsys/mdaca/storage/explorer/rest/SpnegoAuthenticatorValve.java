package com.spinsys.mdaca.storage.explorer.rest;

/**import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.Principal;
import java.security.PrivilegedActionException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.catalina.LifecycleException;
import org.apache.catalina.Realm;
import org.apache.catalina.authenticator.AuthenticatorBase;
import org.apache.catalina.connector.Request;
import org.apache.catalina.deploy.LoginConfig;
import org.ietf.jgss.GSSException;

import net.sourceforge.spnego.SpnegoAuthenticator;
import net.sourceforge.spnego.SpnegoHttpFilter.Constants;
import net.sourceforge.spnego.SpnegoHttpServletResponse;
import net.sourceforge.spnego.SpnegoPrincipal;

public class SpnegoAuthenticatorValve extends AuthenticatorBase {
	
    private static final Logger logger =
            Logger.getLogger("com.spinsys.mdaca.storage.explorer.rest.SpnegoAuthenticatorValve");

    private SpnegoAuthenticator authenticator = null;

    @Override
	public boolean doAuthenticate(Request request, HttpServletResponse response) throws IOException {


        final HttpServletRequest httpRequest = request;
        final SpnegoHttpServletResponse spnegoResponse = new SpnegoHttpServletResponse(response);

            final SpnegoPrincipal principal;
            
            try {

                final Map<String, String> map = new HashMap<String, String>();
                map.put(Constants.ALLOW_BASIC, "false");
                map.put("spnego.allow.localhost", "false");
                map.put("spnego.allow.unsecure.basic", "false");
                map.put("spnego.login.client.module", "spnego-client");
                map.put("spnego.krb5.conf", "");
                map.put("spnego.login.conf", "");
                map.put("spnego.login.server.module", "spnego-server");
                map.put("spnego.preauth.username", Configuration.getConfiguration().getAppConfigurationEntry("spnego-auth")[0].getOptions().get("bindDN").toString());
                map.put("spnego.preauth.password", Configuration.getConfiguration().getAppConfigurationEntry("spnego-auth")[0].getOptions().get("bindCredential").toString());
                map.put("spnego.prompt.ntlm", "false");
                map.put("spnego.allow.delegation", "true");
                map.put("spnego.logger.level", "1");
                map.put("spnego.exclude.dirs", "images");
                
                try {
                    authenticator = new SpnegoAuthenticator(map);
                } catch (LoginException | FileNotFoundException | GSSException | PrivilegedActionException | URISyntaxException e) {
                	logger.log(Level.WARNING, e.getMessage(), e);
                }

                principal = this.authenticator.authenticate(httpRequest, spnegoResponse);
                
            } catch (GSSException e) {
                throw new IOException(e);
            }
            
            // context/auth loop not yet complete
            if (spnegoResponse.isStatusSet()) {
                return false;
            }
            
            //this.
            
            // assert
            if (null == principal) {
                response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                return false;
            }
            
            Realm realm = this.context.getRealm();
            
            String userName = principal.getName();
            
            if(userName.indexOf("@") != -1) {
            	userName = userName.split("@")[0];
            }
            
            // now that we have a username, check if this username has any role(s) defined
            final Principal princ = realm.authenticate(
            		userName, "");
            //realm.
            
            if (null == princ) {
                // username may not have any roles or the wrong roles defined for the
                // the defined security realm (org.apache.catalina.Realm.java)
                response.setStatus(HttpServletResponse.SC_FORBIDDEN);
               return false;
            }
            
            request.setAttribute("spnegoprin", principal);
            
            this.register(request, response, princ, "SPNEGO", princ.getName(), "");

            
        return true;
    }
    
    @Override
    public final void stopInternal() throws LifecycleException {
    	
    	super.stopInternal();

        if (null != this.authenticator) {
            //this.authenticator.dispose();
        }
    }

	@Override
	protected String getAuthMethod() {
		// TODO Auto-generated method stub
		return null;
	}
    


}**/
