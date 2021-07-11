package org.apache.zookeeper;

import java.util.HashMap;
import java.util.Map;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag;


public class JaasConfiguration extends javax.security.auth.login.Configuration {
    private final Map<String, AppConfigurationEntry[]> sections =
      new HashMap<String, AppConfigurationEntry[]>();

    public JaasConfiguration() {
    }

    
    public void addSection(String name, String loginModuleName, String... args) {
        Map<String, String> conf = new HashMap<String, String>();
                for (int i = 0; i < args.length - 1; i += 2) {
            conf.put(args[i], args[i + 1]);
        }
        addSection(name, loginModuleName, conf);
    }

    
    public void addSection(String name, String loginModuleName, final Map<String,String> conf) {
        AppConfigurationEntry[] entries = new AppConfigurationEntry[1];
        entries[0] = new AppConfigurationEntry(loginModuleName, LoginModuleControlFlag.REQUIRED, conf);
        this.sections.put(name, entries);
    }

    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String appName) {
        return sections.get(appName);
    }
}