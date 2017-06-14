package io.mewbase.util;

import java.security.SecureRandom;

/**
 * Used one time to generate the Magic number that appears in the Frame that contains the Bson encoded events.
 */
public class RandomByteGenerator {
    public static void main(String[] args) {
        SecureRandom  numberGenerator = new SecureRandom();
        byte[] randomBytes = new byte[16];
        numberGenerator.nextBytes(randomBytes);
        StringBuilder sb = new StringBuilder();
        for (byte b : randomBytes) {
            sb.append(String.format("(byte)0x%02X,", b));
        }
        System.out.println(sb);
    }
}
