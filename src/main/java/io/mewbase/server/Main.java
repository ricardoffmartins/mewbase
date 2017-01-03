package io.mewbase.server;

import io.mewbase.client.MewException;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

/**
 * Created by tim on 15/12/16.
 */
public class Main {

    private final static Logger logger = LoggerFactory.getLogger(Main.class);

    private static final String CONF_DIR = "conf";
    private static final String CONF_FILE = "mewbase.json";
    private static final String MEWBASE_HOME_SYS_PROP = "mewbase.home";

    public static void main(String[] args) {
        new Main().start();
    }

    private Server server;

    private void start() {

        try {

            String sinstallDir = System.getProperty(MEWBASE_HOME_SYS_PROP);
            if (sinstallDir == null) {
                logger.warn("No mewbase.home specified");
                sinstallDir = ".";
            }
            File installDir = new File(sinstallDir);

            File confDir = new File(installDir, CONF_DIR);
            File confFile = new File(confDir, CONF_FILE);
            ServerOptions options;
            if (!confDir.exists()) {
                logger.warn("Directory " + CONF_DIR + " not found. Using default config instead");
                options = new ServerOptions();
            } else {
                if (!confFile.exists()) {
                    logger.warn("File " + CONF_FILE + " not found in " + CONF_DIR + ". Using default config instead");
                    options = new ServerOptions();
                } else {
                    try (Scanner scanner = new Scanner(confFile).useDelimiter("\\A")) {
                        String sconf = scanner.next();
                        try {
                            JsonObject conf = new JsonObject(sconf);
                            options = jsonToOptions(conf);
                        } catch (DecodeException e) {
                            logger.error("Configuration file " + sconf + " does not contain a valid JSON object");
                            return;
                        }
                    } catch (FileNotFoundException e) {
                        throw new MewException(e);
                    }
                }
            }

            // Adjust the logs and docs dir to point in the distro
            options.setDocsDir(adjustDir(installDir, options.getDocsDir()));
            options.setLogsDir(adjustDir(installDir, options.getLogsDir()));

            server = Server.newServer(options);
            server.start().get();
            while (true) {
                // Stop main exiting
                try {
                    Thread.sleep(Long.MAX_VALUE);
                } catch (InterruptedException ignore) {
                }
            }
        } catch (Exception e) {
            logger.error("Failed to start server", e);
        }
    }

    private String adjustDir(File installDir, String dir) {
        File file = new File(installDir, dir);
        return file.getPath();
    }

    private ServerOptions jsonToOptions(JsonObject conf) {
        return new ServerOptions(conf);
    }
}
