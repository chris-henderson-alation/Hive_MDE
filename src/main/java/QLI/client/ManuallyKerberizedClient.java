package QLI.client;

import QLI.HDFSClient;
import kerberos.Kerberos;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.io.InputStream;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

public class ManuallyKerberizedClient implements Client {

    private final Client rawClient;
    private final Subject subject;

    public ManuallyKerberizedClient(Client rawClient, String username, String password) throws LoginException {
        this.rawClient = rawClient;
        this.subject = Kerberos.kinit(username, password);
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        try {
            return Subject.doAs(
                    this.subject,
                    (PrivilegedExceptionAction<FileStatus[]>) () -> this.rawClient.listStatus(path));
        } catch (PrivilegedActionException e) {
            throw new IOException(e);
        }
    }

    @Override
    public InputStream open(Path path) throws IOException {
        try {
            return Subject.doAs(
                    this.subject,
                    (PrivilegedExceptionAction<InputStream>) () -> this.rawClient.open(path));
        } catch (PrivilegedActionException e) {
            throw new IOException(e);
        }
    }

    @Override
    public Path[] roots() {
        return this.rawClient.roots();
    }
}
