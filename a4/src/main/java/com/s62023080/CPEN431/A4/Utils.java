package com.s62023080.CPEN431.A4;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

public class Utils {

    public static int MAX_REQUEST_SIZE = 16000;

    public static int MAX_MEMORY = 64000000;

    public static long createCheckSum(byte[] messageID, byte[] payload) {
        byte[] checkSum = new byte[messageID.length + payload.length];
        ByteBuffer buffer = ByteBuffer.wrap(checkSum);
        // First bytes are message ID
        buffer.put(messageID);
        // Next bytes are payload
        buffer.put(payload);
        CRC32 crc = new CRC32();
        crc.update(checkSum);
        return crc.getValue();
    }

    public static boolean isCheckSumInvalid(long checkSum, byte[] messageID, byte[] payload) {
        return checkSum != createCheckSum(messageID, payload);
    }

    public static long getFreeMemory() {
        return Runtime.getRuntime().maxMemory() - (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory());
    }

    public static boolean isOutOfMemory() {
        return getFreeMemory() < (Runtime.getRuntime().maxMemory() - MAX_MEMORY);
    }
}