package kerberos;

import org.apache.log4j.Logger;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

public class Kerberos {

    public static final String KRB5_CONF = "krb5.conf";

    private static final String KRB5_CONF_PROPERTY = "java.security.krb5.conf";
    private static final Logger LOGGER = Logger.getLogger(Kerberos.class.getName());

    public static void setKrb5Conf(String pathToConf) {
        LOGGER.info("setting krb5.conf to " + pathToConf);
        System.setProperty(KRB5_CONF_PROPERTY, pathToConf);
    }

    public static Subject kinit(String username, String password) throws LoginException {
        javax.security.auth.login.Configuration.setConfiguration(new Configuration());
        LoginContext lc = new LoginContext(
                Configuration.CONFIGURATION_NAME,
                new kerberos.CallbackHandler(username, password));
        lc.login();
        return lc.getSubject();
    }
}
