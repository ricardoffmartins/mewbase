package io.mewbase.server.impl.log;


import io.mewbase.client.MewException;
import io.mewbase.server.ServerOptions;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.parsetools.RecordParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

/**
 * FileOps provides helper methods over the log files and filesystem
 * Provides the ability to lookup FileCoords i.e. pointers into the Log files at the record level
 * It subsumes and refactors some of the common aspects of LogImpl and LogReadStream
*/

public class FileOps {

    private final static Logger logger = LoggerFactory.getLogger(FileOps.class);

    /**
     * Get a FileSystem handle to a given log file
     * @param logsDir : The directory for the logs
     * @param channel : The channel name
     * @param fileNumber : The log file number
     * @return A file handle or null
     */
    public static File getFile(String logsDir, String channel,int fileNumber) {
        return new File(logsDir, getFileName(channel, fileNumber));
    }

    /**
     * Given a channel and file number make a zero padded unique file name for a log file.
     * @param channel : The channel name
     * @param fileNumber : The file index
     * @return the zero padded file name
     */
    public static String getFileName(String channel, int fileNumber) {
        return channel + "-" + String.format("%012d", fileNumber) + ".log";
    }

    /**
     * Check that the files for this channel are in good order and return the
     * number of the most recently written (highest number)
     * @param options
     * @param channel
     * @return
     */
    public static int checkAndGetLastLogFile(ServerOptions options, String channel) {

        Map<Integer, File> fileMap = new HashMap<>();
        File logDir = new File(options.getLogsDir());
        File[] files = logDir.listFiles(file -> {
            String name = file.getName();
            int lpos = name.lastIndexOf("-");
            if (lpos == -1) {
                logger.warn("Unexpected file in log dir: " + file);
                return false;
            } else {
                String chName = name.substring(0, lpos);
                int num = Integer.valueOf(name.substring(lpos + 1, name.length() - 4));
                boolean matches = chName.equals(channel);
                if (matches) {
                    fileMap.put(num, file);
                }
                return matches;
            }
        });
        if (files == null) {
            throw new MewException("Failed to list files in dir " + logDir.toString());
        }

        Arrays.sort(files, Comparator.naturalOrder());

        // All files before the head file must be right size
        for (int i = 0; i < files.length - 1; i++) {
            if (options.getMaxLogChunkSize() != files[i].length()) {
                throw new MewException("File unexpected size: " + files[i] + " i: " + i +
                        " max log chunk size " + options.getMaxLogChunkSize() + " length " + files[i].length());
            }
        }

        logger.trace("There are {} files in {} for channel {}", files.length, logDir, channel);

        // Check file names are contiguous
        for (int i = 0; i < fileMap.size(); i++) {
            if (!fileMap.containsKey(i)) {
                throw new MewException("Log files not in expected sequence, can't find " + getFileName(channel, i));
            }
        }
        return files.length - 1;
    }

    /**
     * Get the coordinates of the last record in the given log file
     * @param logsDir : Logs Directory
     * @param channel : Events Channel name
     * @param fileNumber : The file number of the given file (normally the head or last file in the channel)
     * @return The File coords of the record.
     */
    public static FileCoord getCoordOfLastRecord(String logsDir, String channel, int fileNumber) {
        // If there is nothing in the log files start from nothing
        File headFile = getFile(logsDir,channel,fileNumber);
        if (!headFile.exists()) {
            return new FileCoord(0L,0,0);
        }

        // TODO
        // Scan the file and find the position of the last record.
        //RecordParser parser = RecordParser.newFixed(HeaderOps.HEADER_SIZE, this::handleHeader);
        return new FileCoord(0,0,0);
    }

    public static FileCoord getCoordPriorToTimestamp(String logsDir, String channel, long timeStamp) {
        return new FileCoord(0,0,0);
    }


    public static FileCoord getCoordOfRecord(String logsDir, String channel, long recordNumber) {
        return new FileCoord(0,0,0);
    }



    private void handleHeader(Buffer buffer) {

    }



    static final class FileCoord {
        final long recordNumber; // the number of the record
        final int fileNumber;    // the number of the file that contains the record
        final int filePos;       // the position in the file of the recoord

        public FileCoord(long recordNumber, int fileNumber, int filePos) {
            this.recordNumber = recordNumber;
            this.fileNumber = fileNumber;
            this.filePos = filePos;
        }
    }



}
