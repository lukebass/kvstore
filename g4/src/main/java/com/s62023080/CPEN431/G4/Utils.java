package com.s62023080.CPEN431.G4;

import com.google.protobuf.ByteString;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.zip.CRC32;
import java.util.concurrent.ConcurrentSkipListMap;

public class Utils {
    private static final int M_BITS = 12;
    public static final int MAX_REQUEST_SIZE = 16000;
    public static final int LOWER_MIN_MEMORY = 5;
    public static final int UPPER_MIN_MEMORY = 10;
    public static final int MAX_CACHE_SIZE = 1000;
    public static final int CACHE_EXPIRATION = 1200;
    public static final int OVERLOAD_TIME = 1500;
    public static final int EPIDEMIC_TIMEOUT = 5000;
    public static final int EPIDEMIC_PERIOD = 300;
    public static final int EPIDEMIC_BUFFER = 10;
    public static final int POP_PERIOD = 300;
    public static final int REPLICATION_FACTOR = 4;
    public static final int PUT_REQUEST = 1;
    public static final int GET_REQUEST = 2;
    public static final int REMOVE_REQUEST = 3;
    public static final int SHUTDOWN_REQUEST = 4;
    public static final int CLEAR_REQUEST = 5;
    public static final int HEALTH_REQUEST = 6;
    public static final int PID_REQUEST = 7;
    public static final int MEMBERSHIP_REQUEST = 8;
    public static final int EPIDEMIC_PUSH = 9;
    public static final int EPIDEMIC_PULL = 10;
    public static final int EPIDEMIC_PUT = 11;
    public static final int KEY_CONFIRMED = 12;
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

    public static ByteString generateMessageID(int port) throws UnknownHostException {
        byte[] messageID = new byte[16];
        ByteBuffer buffer = ByteBuffer.wrap(messageID);
        // First 4 bytes are client IP
        buffer.put(InetAddress.getLocalHost().getAddress());
        // Next 2 bytes are client port
        buffer.putShort((short) port);
        // Next 2 bytes are random
        byte[] random = new byte[2];
        new Random().nextBytes(random);
        buffer.put(random);
        // Next 8 bytes are time
        buffer.putLong(System.nanoTime());
        return ByteString.copyFrom(messageID);
    }

    public static boolean isStoreRequest(int command) {
        return command == Utils.PUT_REQUEST || command == Utils.GET_REQUEST || command == Utils.REMOVE_REQUEST;
    }

    public static boolean isKeyInvalid(ByteString key) {
        return key.size() == 0 || key.size() > 32;
    }

    public static boolean isValueInvalid(ByteString value) {
        return value.size() == 0 || value.size() > 10000;
    }

    public static int searchAddresses(byte[] key, ConcurrentSkipListMap<Integer, Integer> addresses, ArrayList<Integer> replicas) throws IOException {
        int keyID = hashKey(key);

        for (int nodeID : addresses.tailMap(keyID).keySet()) {
            if (!replicas.contains(nodeID)) return nodeID;
        }

        for (int nodeID : addresses.headMap(keyID).keySet()) {
            if (!replicas.contains(nodeID)) return nodeID;
        }

        throw new IOException("Not found");
    }

    public static ConcurrentSkipListMap<Integer, Integer> generateAddresses(ArrayList<Integer> nodes, int weight) {
        ConcurrentSkipListMap<Integer, Integer> addresses = new ConcurrentSkipListMap<>();

        // Iterate over physical nodes
        for (int node : nodes) {
            // Iterate over virtual nodes
            for (int vNode = 1; vNode <= weight; vNode++) {
                // Create (hash, address) pair
                addresses.put(hashNode(node, vNode), node);
            }
        }

        return addresses;
    }

    public static boolean isDeadNode(long time, int size) {
        long threshold = (long) Math.ceil(Utils.EPIDEMIC_TIMEOUT + Utils.EPIDEMIC_PERIOD * ((Math.log(size) / Math.log(2)) + Utils.EPIDEMIC_BUFFER));
        return System.currentTimeMillis() - time > threshold;
    }

    public static int hashKey(byte[] key) {
        CRC32 crc = new CRC32();
        crc.update(key);
        return (int) (crc.getValue() % Math.pow(2, M_BITS));
    }

    public static int hashNode(int node, int vNode) {
        byte[] composite = new byte[8];
        ByteBuffer buffer = ByteBuffer.wrap(composite);
        buffer.putInt(node);
        buffer.putInt(vNode);
        CRC32 crc = new CRC32();
        crc.update(composite);
        return (int) (crc.getValue() % Math.pow(2, M_BITS));
    }

    public static int getFreeMemory() {
        return (int) ((Runtime.getRuntime().maxMemory() - (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory())) / 1024 / 1024);
    }

    public static boolean isOutOfMemory(int threshold) {
        return getFreeMemory() < threshold;
    }
}