package io.mewbase.server.impl;

import java.io.IOException;
import java.io.InputStream;
import java.util.Scanner;



public final class ServerVersionProvider {

    private ServerVersionProvider() {
    }

    private static final String VERSION_TXT = "mewbase-version.txt";

    private static final String VERSION;

    public static String getVersion() {
        return VERSION;
    }

    public static boolean isCompatibleWith(String clientVersion) {
        return compareVersions(clientVersion, getVersion()) <= 0;
    }

    private static int compareVersions(String clientVersion, String serverVersion) {
        try {
            return Double.valueOf(clientVersion).compareTo(Double.valueOf(serverVersion));
        } catch (NumberFormatException e) {
            throw new IllegalStateException(e.getMessage());
        }
    }

    static
    {
        try (InputStream is = ServerVersionProvider.class.getClassLoader()
                                                         .getResourceAsStream(VERSION_TXT)) {
            if (is == null) {
                throw new IllegalStateException("Cannot find " + VERSION_TXT + " on classpath");
            }
            try (Scanner scanner = new Scanner(is, "UTF-8").useDelimiter("\\A")) {
                VERSION = scanner.hasNext() ? scanner.next().trim() : "";
            }
        } catch (IOException e) {
            throw new IllegalStateException(e.getMessage());
        }
    }

}
