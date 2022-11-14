package org.example;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.ftp.FTPFileSystem;

public class MyFTPFileSystem extends FTPFileSystem {

    private Path path;

    @Override
    public Path getWorkingDirectory() {
        if (path != null) return path;
        path = getHomeDirectory();
        return path;
    }
}
