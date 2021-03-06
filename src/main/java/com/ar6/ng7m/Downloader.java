package com.ar6.ng7m;

import org.apache.commons.io.FileUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.client.LaxRedirectStrategy;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

public class Downloader
{
    public File httpDownload(URL url, File dstFile)
    {
        CloseableHttpClient httpclient = HttpClients.custom()
                .setDefaultRequestConfig(RequestConfig.custom()
                .setCookieSpec(CookieSpecs.STANDARD).build())
                .setRedirectStrategy(new LaxRedirectStrategy()).build();

        try
        {
            HttpGet get = new HttpGet(url.toURI()); // we're using GET but it could be via POST as well
            File downloaded = httpclient.execute(get, new FileDownloadResponseHandler(dstFile));
            return downloaded;
        }
        catch (Exception e)
        {
            throw new IllegalStateException(e);
        }
    }

    public boolean FTPDownload(URL url, String localFile)
    {
        boolean isSuccessful;

        try
        {
            FTPDownloader ftpDownloader = new FTPDownloader(url.getHost(),"anonymous","anonymous");

            isSuccessful = ftpDownloader.downloadFile(url.getPath(), localFile );
        }
        catch (Exception e)
        {
            throw new IllegalStateException(e);
        }

        return isSuccessful;
    }

    static class FileDownloadResponseHandler implements ResponseHandler<File>
    {
        private final File target;

        public FileDownloadResponseHandler(File target)
        {
            this.target = target;
        }

        @Override
        public File handleResponse(HttpResponse response) throws IOException
        {
            InputStream source = response.getEntity().getContent();
            FileUtils.copyInputStreamToFile(source, this.target);
            return this.target;
        }
    }
}