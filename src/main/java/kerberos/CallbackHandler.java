package kerberos;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import java.io.IOException;

public class CallbackHandler implements javax.security.auth.callback.CallbackHandler {

    private final String username;
    private final String password;

    public CallbackHandler(String username, String password) {
        this.username = username;
        this.password = password;
    }

    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
        for (Callback c : callbacks) {
            if (c instanceof NameCallback) {
                NameCallback name = ((NameCallback) c);
                name.setName(this.username);
            } else if (c instanceof PasswordCallback) {
                PasswordCallback password = ((PasswordCallback) c);
                password.setPassword(this.password.toCharArray());
            }
        }
    }
}
