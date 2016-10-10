import org.apache.commons.cli.*;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.json.JSONWriter;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.AbstractMap;
import java.util.Date;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;

public class UploadLargeFileTest {

    private static final Logger LOGGER =
            Logger.getLogger(UploadLargeFileTest.class.getName());

    /**
     * Usage: UploadLargeFileTest -o coveochadjohnson01 -s coveochadjohnson01-x4vdgehj6st4bvt6ur3wmx2poi
     *              -a __________-____-____-____-____________ -f "/Users/cjohnson/somefile.pdf"
     *              -d http://www.test.com/somefile.pdf
     *
     * @param args
     */
    public static void main(String[] args) {

        String organizationId = null;
        String sourceId = null;
        String accessToken = null;
        String filePath = null;
        String docId = null;

        // create the command line parser
        CommandLineParser parser = new DefaultParser();

        // create the Options
        Options options = new Options();
        options.addOption(Option.builder("o")
                .longOpt("organization-id")
                .desc("Coveo Organization ID")
                .hasArg().required().build());
        options.addOption(Option.builder("s")
                .longOpt("source-id")
                .desc("Coveo Source ID")
                .hasArg().required().build());
        options.addOption(Option.builder("a")
                .longOpt("access-token")
                .desc("Coveo Source Access Token")
                .hasArg().required().build());
        options.addOption(Option.builder("f")
                .longOpt("file")
                .desc("Path to binary file to upload")
                .hasArg().required().build());
        options.addOption(Option.builder("d")
                .longOpt("doc-id")
                .desc("Coveo Document ID (URI)")
                .hasArg().required().build());

        try {
            // parse the command line arguments
            CommandLine line = parser.parse(options, args);

            organizationId = line.getOptionValue("o");
            sourceId = line.getOptionValue("s");
            accessToken = line.getOptionValue("a");
            filePath = line.getOptionValue("f");
            docId = line.getOptionValue("d");

        } catch (ParseException exp) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("UploadLargeFileTest <args>", options);
            System.exit(0);
        }

        // Construct CoveoPushAPI object
        CoveoPushAPI coveoPushAPI = new CoveoPushAPI(organizationId, sourceId, accessToken);

        try {
            // *** Set Source Status to REBUILD ***
            coveoPushAPI.setSourceStatus("REBUILD");

            // *** Get Pre-Signed AWS S3 URL for uploading file ***
            AbstractMap.SimpleEntry<String, String> s3File = coveoPushAPI.getS3File();
            String uploadUri = s3File.getKey();
            String fileId = s3File.getValue();

            // *** ZLIB compress file ***
            // Open InputStream to binary file
            InputStream is = new FileInputStream(filePath);
            // Preserve original (pre-compressed) byte size, which we will need later
            int originalFileSize = is.available();
            // Create a temp file to hold the compressed file
            File temp = File.createTempFile("temp", ".zlib");
            // wrap input stream with new zlib inputstream
            InputStream zis = coveoPushAPI.zlibInputStream(is, temp);

            // *** PUT the ZLIB file to S3 ***
            coveoPushAPI.putFileOnS3(zis, uploadUri);
            // delete the temp zlib file, which has now been uploaded to S3
            temp.delete();

            // *** Create JSON Document for Coveo
            StringWriter jsonDocument = new StringWriter();
            JSONWriter jw = new JSONWriter(jsonDocument);
            jw.object();

            jw.key("CompressedBinaryDataFileId");
            jw.value(fileId);

            jw.key("size");
            jw.value(originalFileSize);

            jw.key("date");
            Date date = new Date(new File(filePath).lastModified());
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
            jw.value(sdf.format(date));

            jw.key("FileExtension");
            String extension = FilenameUtils.getExtension(filePath);
            // add period to extension; use .txt if no extension was found
            if(StringUtils.isBlank(extension)) {
                extension = ".txt";
            } else {
                extension = "." + extension;
            }
            jw.value(extension);

            jw.endObject();

            String json = jsonDocument.toString();
            LOGGER.info("JSON Document = " + json);

            // *** PUT the JSON Document on Coveo
            coveoPushAPI.putDocumentOnCoveo(json, docId);

            // *** Set source status back to IDLE
            coveoPushAPI.setSourceStatus("IDLE");


        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to push file", e);
        }

    }

}
