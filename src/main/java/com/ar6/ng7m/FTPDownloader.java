package com.ar6.ng7m;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.commons.net.PrintCommandListener;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;

public class FTPDownloader
{

    FTPClient ftp;

    public FTPDownloader(String host, String user, String pwd) throws Exception
    {
        ftp = new FTPClient();
        ftp.addProtocolCommandListener(new PrintCommandListener(new PrintWriter(System.out)));
        int reply;

        ftp.connect(host);
        reply = ftp.getReplyCode();

        if (!FTPReply.isPositiveCompletion(reply))
        {
            ftp.disconnect();
            throw new Exception("FTPDownloader: Exception in connecting to FTP Server: " + host);
        }

        ftp.login(user, pwd);
        ftp.setFileType(FTP.BINARY_FILE_TYPE);
        ftp.enterLocalPassiveMode();
    }

    public boolean downloadFile(String remoteFile, String localFile) throws Exception
    {
        boolean downloadSuccessful;

        try (FileOutputStream fileOutputStream = new FileOutputStream(localFile))
        {
            downloadSuccessful = ftp.retrieveFile(remoteFile, fileOutputStream);
        }
        catch (IOException e)
        {
            throw new Exception("FTPDownloader: Exception in FTP get downloading file path: " + remoteFile + " File: " + localFile);
        }
        finally
        {
            disconnect();
        }

        return downloadSuccessful;
    }

    public void disconnect()
    {
        if (this.ftp.isConnected())
        {
            try
            {
                this.ftp.logout();
                this.ftp.disconnect();
            }
            catch (IOException f)
            {
                // do nothing as file is already downloaded from FTP server
            }
        }
    }

}