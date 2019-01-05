package kerberos;


import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.io.IOException;

public class Kerberos {

    private static final String LOGIN_MODULE = "primaryLoginContext";
    private static final String AUTH_LOGIN_CONFIGURATION = "java.security.auth.login.config";
    private static final String KRB_DEBUG = "sun.security.krb5.debug";
    private static final String KRB_CONF = "/Users/chris.henderson/alation/externals/alation/adbc/java/src/alation/resources/kerberos.conf";

    static {
        System.setProperty(AUTH_LOGIN_CONFIGURATION, KRB_CONF);
        System.setProperty( "java.security.krb5.conf", "/etc/krb5.conf");
        System.setProperty(KRB_DEBUG, "false");
    }


    public static LoginContext kinit(String username, String password) throws LoginException {
        LoginContext lc = new LoginContext(LOGIN_MODULE, new CallbackHandler() {
            public void handle(Callback[] callbacks) throws IOException, UnsupportedOperationException {
                for(Callback c : callbacks){
                    if(c instanceof NameCallback)
                        ((NameCallback) c).setName(username);
                    if(c instanceof PasswordCallback)
                        ((PasswordCallback) c).setPassword(password.toCharArray());
                }
            }});
        lc.login();
        return lc;
    }

}
