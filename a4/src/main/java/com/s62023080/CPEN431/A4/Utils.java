package com.s62023080.CPEN431.A4;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.zip.CRC32;
import java.util.concurrent.ConcurrentSkipListMap;

public class Utils {
    private static final int M_BITS = 11;
    public static final int MAX_REQUEST_SIZE = 16000;
    public static final int MAX_MEMORY = 55;
    public static final int MAX_CACHE_SIZE = 1000;
    public static final int CACHE_EXPIRATION = 1000;
    public static final int OVERLOAD_TIME = 1000;
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

    /**
     * Determine if request key is between two node hashes
     *
     * @param keyID the request key hash
     * @param firstID the first node hash
     * @param secondID the second node hash
     * @return boolean indicating if key is between
     */
    public static boolean inBetween(int keyID, int firstID, int secondID) {
        if (secondID > firstID) return keyID >= firstID && keyID < secondID;
        else if (keyID >= firstID && keyID < secondID + Math.pow(2, M_BITS)) return true;
        else return keyID + Math.pow(2, M_BITS) >= firstID && keyID < secondID;
    }

    /**
     * Constructs mapping of node hashes and addresses
     *
     * @param nodes the physical node addresses in cluster
     * @param weight the weight of virtual nodes
     * @return ConcurrentSkipListMap of (hash, address) pairs
     */
    public static ConcurrentSkipListMap<Integer, Integer> generateAddresses(List<Integer> nodes, int weight) {
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

    /**
     * Constructs finger tables for virtual nodes at physical node
     *
     * @param node the physical node address
     * @param weight the weight of virtual nodes
     * @param nodeSet sorted list of node hashes
     * @return ConcurrentSkipListMap of node hashes and finger tables
     */
    public static ConcurrentSkipListMap<Integer, int[]> generateTables(int node, int weight, List<Integer> nodeSet) {
        ConcurrentSkipListMap<Integer, int[]> tables = new ConcurrentSkipListMap<>();

        // Iterate over virtual nodes
        for (int vNode = 1; vNode <= weight; vNode++) {
            int nodeID = hashNode(node, vNode);
            int[] table = new int[M_BITS + 1];
            // Set first finger to be predecessor
            table[0] = nodeSet.get(nodeSet.indexOf(nodeID) == 0 ? nodeSet.size() - 1 : nodeSet.indexOf(nodeID) - 1);
            // Set remaining fingers to be successors
            for (int i = 1; i < M_BITS + 1; i++) table[i] = generateFinger(i, nodeID, nodeSet);
            tables.put(nodeID, table);
        }

        return tables;
    }

    /**
     * Determines finger for the given index
     *
     * @param index the index for finger generation
     * @param nodeID the virtual node hash
     * @param nodeSet sorted list of node hashes
     * @return Finger for the given index
     */
    public static int generateFinger(int index, int nodeID, List<Integer> nodeSet) {
        // Get the successor position
        int successor = (int) ((nodeID + Math.pow(2, index - 1)) % Math.pow(2, M_BITS));
        // Get the current position
        int curr = nodeSet.indexOf(nodeID);
        // Get the next position
        int next = (curr + 1) % nodeSet.size();

        // Iterate over nodeSet
        int finger = 0;
        for (int i = 0; i < nodeSet.size(); i++) {
            // Check if successor node is found
            if (inBetween(successor, nodeSet.get(curr) + 1, nodeSet.get(next) + 1)) {
                finger = nodeSet.get(next);
                break;
            }

            // Update indices one ahead
            curr = next;
            next = (next + 1) % nodeSet.size();
        }

        return finger;
    }

    /**
     * Generates hash for request key
     *
     * @param key the request key
     * @return hash for request key
     */
    public static int hashKey(byte[] key) {
        CRC32 crc = new CRC32();
        crc.update(key);
        return (int) (crc.getValue() % Math.pow(2, M_BITS));
    }

    /**
     * Generates hash for virtual node
     *
     * @param node the physical node
     * @param vNode the virtual node
     * @return hash for virtual node
     */
    public static int hashNode(int node, int vNode) {
        byte[] composite = new byte[8];
        ByteBuffer buffer = ByteBuffer.wrap(composite);
        buffer.putInt(node);
        buffer.putInt(vNode);
        CRC32 crc = new CRC32();
        crc.update(composite);
        return (int) (crc.getValue() % Math.pow(2, M_BITS));
    }

    public static long getFreeMemory() {
        return (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / 1024 / 1024;
    }

    public static boolean isOutOfMemory() {
        return getFreeMemory() > MAX_MEMORY;
    }
}