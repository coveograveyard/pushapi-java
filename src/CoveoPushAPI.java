import org.json.JSONObject;
import org.json.JSONTokener;

import javax.net.ssl.HttpsURLConnection;
import java.io.*;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.logging.Logger;
import java.util.zip.DeflaterOutputStream;

public class CoveoPushAPI {

    private String organizationId;
    private String sourceId;
    private String accessToken;

    private static final Logger LOGGER =
            Logger.getLogger(CoveoPushAPI.class.getName());

    public CoveoPushAPI(String organizationId, String sourceId, String accessToken) {
        this.organizationId = organizationId;
        this.sourceId = sourceId;
        this.accessToken = accessToken;
    }

    /**
     * Set Coveo Source Status to <code>statusType</code>
     *
     * @param statusType
     */
    public void setSourceStatus(String statusType) throws Exception {

        // Build the "status" API URL
        URL url = new URL("https://push.cloud.coveo.com/v1/organizations/" + organizationId + "/sources/" + sourceId + "/status?statusType=" + statusType);
        LOGGER.info("=>> Setting Source Status: " + url.toString());

        HttpsURLConnection conn = (HttpsURLConnection) url.openConnection();

        // Add the Authorization header with the accessToken
        conn.setRequestProperty("Authorization", "Bearer " + accessToken);

        // Set parameters of the HttpURLConnection
        conn.setReadTimeout(10000);
        conn.setConnectTimeout(15000);
        conn.setRequestMethod("POST");
        conn.setDoInput(true);
        conn.setDoOutput(true);

        // Open the URL connection
        conn.connect();

        int responseCode = conn.getResponseCode();
        LOGGER.info("<<= Response Code = " + responseCode + " " + conn.getResponseMessage());

        if (responseCode != 201) {
            // If the request failed in some way. Check the error stream for information
            InputStream is = conn.getErrorStream();
            LOGGER.severe("Error = " + readStream(is));
            throw new Exception("Unable to set source status");
        }

    }

    /**
     * Ask Coveo Files API for a pre-signed AWS S3 URL, and a fileID for referencing it in the JSON Document
     *
     * @return
     */
    public AbstractMap.SimpleEntry<String, String> getS3File() throws Exception {

        URL url = new URL("https://push.cloud.coveo.com/v1/organizations/" + organizationId + "/files");
        LOGGER.info("=>> Getting S3 File Info: " + url.toString());

        HttpsURLConnection conn = (HttpsURLConnection) url.openConnection();

        // Add the Authorization header with the accessToken
        conn.setRequestProperty("Authorization", "Bearer " + accessToken);

        // Set parameters of the HttpURLConnection
        conn.setReadTimeout(10000);
        conn.setConnectTimeout(15000);
        conn.setRequestMethod("POST");
        conn.setDoInput(true);
        conn.setDoOutput(true);

        // Open the URL connection
        conn.connect();

        int responseCode = conn.getResponseCode();
        LOGGER.info("<<= Response Code = " + responseCode + " " + conn.getResponseMessage());

        if (responseCode == 201) {
            // If successful, read the response, which contains JSON information
            InputStream is = conn.getInputStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(is));
            String output = reader.readLine();
            LOGGER.info("<<= Response Body: " + output);

            // Parse the JSON
            JSONTokener tokener = new JSONTokener(output);
            JSONObject root = new JSONObject(tokener);

            // Extract the uploadUri and fileId
            String uploadUri = root.getString("uploadUri");
            String fileId = root.getString("fileId");

            // return those two values
            return new AbstractMap.SimpleEntry<>(uploadUri, fileId);
        } else {
            // Else the request failed in some way. Check the error stream for information
            InputStream is = conn.getErrorStream();
            LOGGER.severe("Error = " + readStream(is));
            throw new Exception("Unable to fetching S3 URL");
        }
    }

    /**
     * Compress (zlib) an inputstream into a temp file, and return a new inputstream for reading the zlib'ed file.
     * The original inputstream will be closed.
     * It is the responsibility of the caller to delete the temp file after consuming it.
     *
     * @param original
     * @param temp
     * @return inputstream for reading the zlip'ed temp file
     * @throws Exception
     */
    public InputStream zlibInputStream(InputStream original, File temp) throws Exception {

        LOGGER.info("Zlib inputstream to temp file: " + temp);
        FileOutputStream fos = new FileOutputStream(temp);
        DeflaterOutputStream dos = new DeflaterOutputStream(fos);

        doCopy(original, dos); // copy original stream to temp.zlib

        FileInputStream zis = new FileInputStream(temp);
        return zis;
    }


    /**
     * PUT the zlib'ed file on S3
     *
     * @param in an InputStream that reads from a zlib-compressed file
     * @param uploadUri a pre-signed AWS S3 upload url
     */
    public void putFileOnS3(InputStream in, String uploadUri) throws Exception {
        URL url = new URL(uploadUri);
        LOGGER.info("=>> PUT file to S3: " + url.toString());
        HttpsURLConnection conn = (HttpsURLConnection) url.openConnection();

        // Add the required headers
        conn.setRequestProperty("Content-Type", "application/octet-stream");
        conn.setRequestProperty("x-amz-server-side-encryption", "AES256");

        // Set parameters of the HttpURLConnection
        conn.setReadTimeout(10000);
        conn.setConnectTimeout(15000);
        conn.setRequestMethod("PUT");
        conn.setDoInput(true);
        conn.setDoOutput(true);

        // write the zlib file to the URL Connection stream
        BufferedOutputStream bos = new BufferedOutputStream(conn.getOutputStream());
        try {
            BufferedInputStream bis = new BufferedInputStream(in);
            int i;
            while ((i = bis.read()) >= 0) {
                bos.write(i);
            }
            bos.flush();
        } finally {
            bos.close();
        }

        int responseCode = conn.getResponseCode();
        LOGGER.info("<<= Response Code = " + responseCode + " " + conn.getResponseMessage());

        if (responseCode != 200) {
            // If the request failed in some way. Check the error stream for information
            InputStream is = conn.getErrorStream();
            LOGGER.severe("Error = " + readStream(is));
            throw new Exception("Unable to upload file to S3");
        }
    }

    /**
     * PUT the provided JSON Document on Coveo
     * @param json the JSON Document
     * @param docId the unique Coveo documentId / URI
     */
    public void putDocumentOnCoveo(String json, String docId) throws Exception {
        try {
            // append docId (url-encoded to be safe)
            URL url = new URL("https://push.cloud.coveo.com/v1/organizations/" + organizationId + "/sources/" + sourceId + "/documents?documentId=" + URLEncoder.encode(docId, "UTF-8"));
            LOGGER.info("=>> PUT document to Coveo: " + url.toString());
            HttpsURLConnection conn = (HttpsURLConnection) url.openConnection();

            // Set authorization and content-type headers
            conn.setRequestProperty("Authorization", "Bearer " + accessToken);
            conn.setRequestProperty("content-type", "application/json");

            // Set parameters of the HttpURLConnection
            conn.setReadTimeout(10000);
            conn.setConnectTimeout(15000);
            conn.setRequestMethod("PUT");
            conn.setDoInput(true);
            conn.setDoOutput(true);

            BufferedOutputStream bos = new BufferedOutputStream(conn.getOutputStream());
            BufferedInputStream bis = new BufferedInputStream(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)));
            int i;
            // read byte by byte until end of stream
            while ((i = bis.read()) >= 0) {
                bos.write(i);
            }
            bos.close();

            int responseCode = conn.getResponseCode();
            LOGGER.info("<<= Response Code = " + responseCode + " " + conn.getResponseMessage());

            if (responseCode != 202) {
                // If the request failed in some way. Check the error stream for information
                InputStream is = conn.getErrorStream();
                LOGGER.severe("Error = " + readStream(is));
                throw new Exception("Unable to upload JSON Document to Coveo");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * DELETE the specified documentId / URI on Coveo
     * @param docId the Coveo documentId / URI
     */
    public void deleteDocumentOnCoveo(String docId) throws Exception {
        try {
            // append docId (url-encoded to be safe)
            URL url = new URL("https://push.cloud.coveo.com/v1/organizations/" + organizationId + "/sources/" + sourceId + "/documents?documentId=" + URLEncoder.encode(docId, "UTF-8"));
            LOGGER.info("=>> DELETE document to Coveo: " + url.toString());
            HttpsURLConnection conn = (HttpsURLConnection) url.openConnection();

            // Set authorization and content-type headers
            conn.setRequestProperty("Authorization", "Bearer " + accessToken);
            conn.setRequestProperty("content-type", "application/json");

            // Set parameters of the HttpURLConnection
            conn.setReadTimeout(10000);
            conn.setConnectTimeout(15000);
            conn.setRequestMethod("DELETE");
            conn.setDoInput(true);
            conn.setDoOutput(true);

            int responseCode = conn.getResponseCode();
            LOGGER.info("<<= Response Code = " + responseCode + " " + conn.getResponseMessage());

            if (responseCode != 202) {
                // If the request failed in some way. Check the error stream for information
                InputStream is = conn.getErrorStream();
                LOGGER.severe("Error = " + readStream(is));
                throw new Exception("Unable to delete document on Coveo");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    /**
     * Copy inputstream to outputstream
     *
     * @param is
     * @param os
     * @throws Exception
     */
    private void doCopy(InputStream is, OutputStream os) throws Exception {
        byte[] bytes = new byte[1024];
        int length;
        while ((length = is.read(bytes)) >= 0) {
            os.write(bytes, 0, length);
        }
        os.close();
        is.close();
    }

    /**
     * Read inputstream into a String
     * @param stream
     * @return
     * @throws Exception
     */
    private String readStream(InputStream stream) throws Exception {
        StringBuilder builder = new StringBuilder();
        try (BufferedReader in = new BufferedReader(new InputStreamReader(stream))) {
            String line;
            while ((line = in.readLine()) != null) {
                builder.append(line).append("\n"); // + "\r\n"(no need, json has no line breaks!)
            }
            in.close();
        }
        return builder.toString();
    }

}
