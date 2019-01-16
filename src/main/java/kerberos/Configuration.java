package kerberos;

import com.google.common.collect.ImmutableMap;

import javax.security.auth.login.AppConfigurationEntry;

public class Configuration extends javax.security.auth.login.Configuration {

    public static final String CONFIGURATION_NAME = "hiveLoginContext";
    private static final AppConfigurationEntry[] JAAS_CONFIGURATION = new AppConfigurationEntry[]{
            new AppConfigurationEntry(
                    "com.sun.security.auth.module.Krb5LoginModule",
                    AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                    ImmutableMap.of(
                            "refreshKrb5Config", "false",
                            "useTicketCache", "false",
                            "debug", "false"
                    )
            )};

    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
        if (!CONFIGURATION_NAME.equals(name)) {
            throw new RuntimeException("@TODO some message about how you programmed the wrong JAAS name");
        }
        return JAAS_CONFIGURATION;
    }
}
