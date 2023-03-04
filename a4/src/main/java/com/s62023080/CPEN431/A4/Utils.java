package com.s62023080.CPEN431.A4;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

public class Utils {
    public static final int MAX_REQUEST_SIZE = 16000;
    public static final int MAX_MEMORY = 55;
    public static final int MAX_CACHE_SIZE = 1000;
    public static final int PUT_REQUEST = 1;
    public static final int GET_REQUEST = 2;
    public static final int REMOVE_REQUEST = 3;
    public static final int SHUTDOWN_REQUEST = 4;
    public static final int CLEAR_REQUEST = 5;
    public static final int HEALTH_REQUEST = 6;
    public static final int PID_REQUEST = 7;
    public static final int MEMBERSHIP_REQUEST = 8;
    public static final int SUCCESS = 0;
    public static final int MISSING_KEY_ERROR = 1;
    public static final int MEMORY_ERROR = 2;
    public static final int OVERLOAD_ERROR = 3;
    public static final int STORE_ERROR = 4;
    public static final int UNRECOGNIZED_ERROR = 5;
    public static final int INVALID_KEY_ERROR = 6;
    public static final int INVALID_VALUE_ERROR = 7;

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
        return (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / 1024 / 1024;
    }

    public static boolean isOutOfMemory() {
        return getFreeMemory() > MAX_MEMORY;
    }
}