package com.s62023080.CPEN431.G4;

import com.google.protobuf.ByteString;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Random;
import java.util.zip.CRC32;
import java.util.concurrent.ConcurrentSkipListMap;

public class Utils {
    private static final int M_BITS = 12;
    public static final int MAX_REQUEST_SIZE = 16000;
    public static final int LOWER_MIN_MEMORY = 5;
    public static final int UPPER_MIN_MEMORY = 10;
    public static final int MAX_CACHE_SIZE = 1000;
    public static final int CACHE_EXPIRATION = 1000;
    public static final int OVERLOAD_TIME = 1500;
    public static final int EPIDEMIC_TIMEOUT = 5000;
    public static final int EPIDEMIC_PERIOD = 300;
    public static final int EPIDEMIC_BUFFER = 10;
    public static final int POP_PERIOD = 300;
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

    /**
     * Determine if request key is between two node hashes
     *
     * @param keyID the request key hash
     * @param firstID the first node hash
     * @param secondID the second node hash
     * @return boolean indicating if key is between
     */
    public static boolean inBetween(int keyID, int firstID, int secondID) {
        if (firstID == secondID) return false;
        else if (secondID > firstID) return keyID >= firstID && keyID < secondID;
        return keyID >= firstID && keyID < secondID + Math.pow(2, M_BITS)
                || keyID + Math.pow(2, M_BITS) >= firstID && keyID < secondID;
    }

    /**
     * Determine if key belongs to local node
     *
     * @param key the request key
     * @param tables ConcurrentSkipListMap of node hashes and finger tables
     * @return boolean indicating if key is local
     */
    public static boolean isLocalKey(byte[] key, ConcurrentSkipListMap<Integer, int[]> tables) {
        int keyID = hashKey(key);

        // Iterate over virtual nodes
        boolean found = false;
        for (int nodeID : tables.keySet()) {
            // Get finger table for current virtual node
            int[] table = tables.get(nodeID);
            // Check if current node is responsible
            if (inBetween(keyID, table[0] + 1, nodeID + 1)) {
                found = true;
                break;
            }
        }

        return found;
    }

    /**
     * Search finger tables to find key location
     *
     * @param key the request key
     * @param tables ConcurrentSkipListMap of node hashes and finger tables
     * @return node hash to search next
     */
    public static int searchTables(byte[] key, ConcurrentSkipListMap<Integer, int[]> tables) {
        int keyID = hashKey(key);

        // Iterate over virtual nodes
        int nodeID = 0;
        ArrayList<Integer> nodes = new ArrayList<>(tables.keySet());
        for (int i = 0; i < nodes.size(); i++) {
            // Check if virtual node is responsible
            if (inBetween(keyID, nodes.get(i), nodes.get((i + 1) % nodes.size()))) {
                nodeID = nodes.get(i);
                break;
            }
        }

        // Get finger table for closest node to key
        int[] table = tables.get(nodeID);
        // Check if successor node is responsible
        if (inBetween(keyID, nodeID + 1, table[1] + 1)) return table[1];

        // Iterate over finger table
        for (int i = 1; i < M_BITS + 1; i++) {
            // Check if finger node is responsible
            if (inBetween(keyID, table[i], table[(i + 1) % (M_BITS + 1)])) {
                nodeID = table[i];
                break;
            }
        }

        return nodeID;
    }

    /**
     * Constructs mapping of node hashes and addresses
     *
     * @param nodes the physical node addresses in cluster
     * @param weight the weight of virtual nodes
     * @return ConcurrentSkipListMap of (hash, address) pairs
     */
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

    /**
     * Constructs finger tables for virtual nodes at physical node
     *
     * @param nodes sorted list of node hashes
     * @param node the physical node address
     * @param weight the weight of virtual nodes
     * @return ConcurrentSkipListMap of node hashes and finger tables
     */
    public static ConcurrentSkipListMap<Integer, int[]> generateTables(ArrayList<Integer> nodes, int node, int weight) {
        ConcurrentSkipListMap<Integer, int[]> tables = new ConcurrentSkipListMap<>();

        // Iterate over virtual nodes
        for (int vNode = 1; vNode <= weight; vNode++) {
            int nodeID = hashNode(node, vNode);
            int[] table = new int[M_BITS + 1];
            // Set first finger to be predecessor
            table[0] = nodes.get(nodes.indexOf(nodeID) == 0 ? nodes.size() - 1 : nodes.indexOf(nodeID) - 1);
            // Set remaining fingers to be successors
            for (int i = 1; i < M_BITS + 1; i++) table[i] = generateFinger(i, nodeID, nodes);
            tables.put(nodeID, table);
        }

        return tables;
    }

    /**
     * Determines finger for the given index
     *
     * @param index the index for finger generation
     * @param nodeID the virtual node hash
     * @param nodes sorted list of node hashes
     * @return finger for the given index
     */
    public static int generateFinger(int index, int nodeID, ArrayList<Integer> nodes) {
        // Get the successor position
        int successor = (int) ((nodeID + Math.pow(2, index - 1)) % Math.pow(2, M_BITS));
        // Get the current position
        int curr = nodes.indexOf(nodeID);
        // Get the next position
        int next = (curr + 1) % nodes.size();

        // Iterate over nodes
        int finger = 0;
        for (int i = 0; i < nodes.size(); i++) {
            // Check if successor node is found
            if (inBetween(successor, nodes.get(curr) + 1, nodes.get(next) + 1)) {
                finger = nodes.get(next);
                break;
            }

            // Update indices one ahead
            curr = next;
            next = (next + 1) % nodes.size();
        }

        return finger;
    }

    public static boolean isDeadNode(long time, int size) {
        long threshold = (long) Math.ceil(Utils.EPIDEMIC_TIMEOUT + Utils.EPIDEMIC_PERIOD * ((Math.log(size) / Math.log(2)) + Utils.EPIDEMIC_BUFFER));
        return System.currentTimeMillis() - time > threshold;
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

    public static int getFreeMemory() {
        return (int) ((Runtime.getRuntime().maxMemory() - (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory())) / 1024 / 1024);
    }

    public static boolean isOutOfMemory(int threshold) {
        return getFreeMemory() < threshold;
    }
}