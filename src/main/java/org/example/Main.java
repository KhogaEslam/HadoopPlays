package org.example;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.ftp.FTPFileSystem;

import java.io.IOException;
import java.net.URI;

public class Main {
    private static final String host = "";
    private static final int port = 21;
    private static final String username = "";
    private static final String password = "";

    public static void main(String[] args) {
//        connectFTP();
        useHaddoopFS();
//        System.out.println(checkLoopReturn());
    }

    private static String checkLoopReturn() {
        int i = 3;
        do {
            System.out.println(i);
            if (i == 2) return "2";
            i--;
        } while(i > 0);
        return "Any";
    }

    private static void useHaddoopFS() {
        try {
            Configuration conf = new Configuration(false);

//            System.out.println(System.getProperty("javax.security.auth.useSubjectCredsOnly"));
//            System.setProperty("javax.security.auth.useSubjectCredsOnly", "true");
//            conf.set("javax.security.auth.useSubjectCredsOnly", "true");
//            conf.addResource("core-default.xml");

            conf.set("fs.ftp.host", host);
            conf.set("fs.ftp.host.port", String.valueOf(port));
            conf.set("fs.ftp.user." + host, username);
            conf.set("fs.ftp.password." + host, password);
            conf.set("fs.ftp.data.connection.mode", "PASSIVE_LOCAL_DATA_CONNECTION_MODE");
//            conf.set("fs.ftp.data.connection.mode", "PASSIVE_REMOTE_DATA_CONNECTION_MODE");

//            conf.set("fs.ftp.transfer.mode", "COMPRESSED_TRANSFER_MODE");
            conf.set("fs.ftp.transfer.mode", "STREAM_TRANSFER_MODE");

            conf.set("fs.ftp.impl", "org.apache.hadoop.fs.ftp.FTPFileSystem");
//            conf.set("fs.ftp.impl", "org.example.MyFTPFileSystem");
//            conf.set("fs.AbstractFileSystem.ftp.impl", "org.example.MyFTPFileSystem");

//            String fsURL = String.format("ftp://%s:%s/", host, String.valueOf(port));
//            conf.set("fs.defaultFS", fsURL);

//            conf.set("ftp.stream-buffer-size", "8192");
//            conf.set("ftp.client-write-packet-size", "131072");
//            conf.set("ftp.blocksize", "134217728");
//            conf.set("hadoop.root.logger", "ALL,console");
//            conf.set("log4j.logger.org.apache.hadoop", "ALL");
//            conf.set("fs.ftp.timeout", "1000");

//            System.setProperty("jdk.tls.useExtendedMasterSecret", "false");

            System.out.println(conf);
//            try (FileSystem fs = new FTPFileSystem();) {
//                fs.initialize(URI.create(fsURL), conf);
                FileSystem fs = FileSystem.newInstance(conf);
                System.out.println("0 =====");
                Path somePath = new Path("ftp://" + host + ":" + port + "//");
                Path somePath2 = new Path("/");
                System.out.println("1 =====");
                System.out.println(fs.getFileStatus(somePath).isDirectory()); // returns true
                System.out.println("2 =====");
                System.out.println(fs.getScheme());
                System.out.println(fs.getFileStatus(somePath).getLen());
                System.out.println(fs.getWorkingDirectory());
                System.out.println(fs.getHomeDirectory());
                System.out.println("3 =====");
                FsStatus fStatus = fs.getStatus(somePath);
                System.out.println(fStatus);
                System.out.println("4 =====");
                FileStatus[] stss = fs.listStatus(somePath); // keeps spinning then throws SocketTimeOutException
                System.out.println("5 =====");
                for (FileStatus file : stss) {
                    System.out.println(file.getPath().getName());
                }
//            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void connectFTP() {
        FTPClient ftpClient = new FTPClient();
        try {
            ftpClient.connect(host, port);
            showServerReply(ftpClient);
            int replyCode = ftpClient.getReplyCode();
            if (!FTPReply.isPositiveCompletion(replyCode)) {
                System.out.println("Operation failed. Server reply code: " + replyCode);
                return;
            }
            boolean success = ftpClient.login(username, password);
            showServerReply(ftpClient);
            if (!success) {
                System.out.println("Could not login to the server");
                return;
            } else {
                System.out.println("LOGGED IN SERVER");
                System.out.println(ftpClient.getStatus("/"));
            }
        } catch (IOException ex) {
            System.out.println("Oops! Something wrong happened");
            ex.printStackTrace();
        } finally {
            try {
                ftpClient.logout();
                ftpClient.disconnect();
                System.out.println("disconnected");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static void showServerReply(FTPClient ftpClient) {
        System.out.println(ftpClient.getReplyCode());
        String[] replies = ftpClient.getReplyStrings();
        if (replies != null && replies.length > 0) {
            for (String aReply : replies) {
                System.out.println("SERVER: " + aReply);
            }
        }
    }
}
