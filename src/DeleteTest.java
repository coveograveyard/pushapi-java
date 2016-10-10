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

public class DeleteTest {

    private static final Logger LOGGER =
            Logger.getLogger(DeleteTest.class.getName());

    /**
     * Usage: DeleteTest -o coveochadjohnson01 -s coveochadjohnson01-x4vdgehj6st4bvt6ur3wmx2poi
     *              -a __________-____-____-____-____________ -d http://www.test.com/somefile.pdf
     *
     * @param args
     */
    public static void main(String[] args) {

        String organizationId = null;
        String sourceId = null;
        String accessToken = null;
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
            docId = line.getOptionValue("d");

        } catch (ParseException exp) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("DeleteTest <args>", options);
            System.exit(0);
        }

        // Construct CoveoPushAPI object
        CoveoPushAPI coveoPushAPI = new CoveoPushAPI(organizationId, sourceId, accessToken);

        try {
            // *** Set Source Status to REBUILD ***
            coveoPushAPI.setSourceStatus("REBUILD");

            // *** PUT the JSON Document on Coveo
            coveoPushAPI.deleteDocumentOnCoveo(docId);

            // *** Set source status back to IDLE
            coveoPushAPI.setSourceStatus("IDLE");


        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to push file", e);
        }

    }

}
