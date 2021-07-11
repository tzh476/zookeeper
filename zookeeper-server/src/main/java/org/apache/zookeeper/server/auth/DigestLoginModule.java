package org.apache.zookeeper.server.auth;

import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.spi.LoginModule;
import java.util.Map;

public class DigestLoginModule implements LoginModule {
    private Subject subject;

    public boolean abort() {
        return false;
    }

    public boolean commit() {
        return true;
    }

    public void initialize(Subject subject, CallbackHandler callbackHandler, Map<String,?> sharedState, Map<String,?> options) {
        if (options.containsKey("username")) {
                        this.subject = subject;
            String username = (String)options.get("username");
            this.subject.getPublicCredentials().add((Object)username);
            String password = (String)options.get("password");
            this.subject.getPrivateCredentials().add((Object)password);
        }
        return;
    }

    public boolean logout() {
        return true;
    }

    public boolean login() {
                        return true;
    }

}


