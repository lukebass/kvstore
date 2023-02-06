package com.s62023080.CPEN431.A4;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

public class Utils {

    public static int MAX_REQUEST_SIZE = 16000;

    public static int MAX_CACHE_SIZE = 1000;

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

    public static boolean isOutOfMemory() {
        long used = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        long free = Runtime.getRuntime().maxMemory() - used;
        return free < (MAX_REQUEST_SIZE * 100L);
    }
}