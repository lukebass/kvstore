package com.s62023080.CPEN431.G4;

import ca.NetSysLab.ProtocolBuffers.KeyValueRequest.KVRequest;
import ca.NetSysLab.ProtocolBuffers.KeyValueResponse.KVResponse;
import com.google.protobuf.ByteString;
import ca.NetSysLab.ProtocolBuffers.Message.Msg;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.*;

public class Utils {
    private static final int M_BITS = 12;
    public static final int MAX_REQUEST_SIZE = 16000;
    public static final int LOWER_MIN_MEMORY = 5;
    public static final int UPPER_MIN_MEMORY = 10;
    public static final int MAX_CACHE_SIZE = 1000;
    public static final int CACHE_EXPIRATION = 1000;
    public static final int OVERLOAD_TIME = 1000;
    public static final int EPIDEMIC_TIMEOUT = 10000;
    public static final int EPIDEMIC_PERIOD = 300;
    public static final int EPIDEMIC_BUFFER = 10;
    public static final int QUEUE_EXPIRATION = 1500;
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
    public static final int REPLICA_PUSH = 11;
    public static final int REPLICA_CONFIRMED = 12;
    public static final int SUCCESS = 0;
    public static final int MISSING_KEY_ERROR = 1;
    public static final int MEMORY_ERROR = 2;
    public static final int OVERLOAD_ERROR = 3;
    public static final int STORE_ERROR = 4;
    public static final int UNRECOGNIZED_ERROR = 5;
    public static final int INVALID_KEY_ERROR = 6;
    public static final int INVALID_VALUE_ERROR = 7;

    public static boolean isNodeRequest(int command) {
        return command == Utils.SHUTDOWN_REQUEST || command == Utils.CLEAR_REQUEST || command == Utils.HEALTH_REQUEST || command == Utils.PID_REQUEST || command == Utils.MEMBERSHIP_REQUEST;
    }

    public static boolean isEpidemicRequest(int command) {
        return command == Utils.EPIDEMIC_PUSH || command == Utils.EPIDEMIC_PULL;
    }

    public static boolean isReplicaRequest(int command) {
        return command == Utils.REPLICA_PUSH || command == Utils.REPLICA_CONFIRMED;
    }

    public static boolean isChangeRequest(int command) {
        return command == Utils.PUT_REQUEST || command == Utils.REMOVE_REQUEST;
    }

    public static boolean isKeyInvalid(ByteString key) {
        return key.size() == 0 || key.size() > 32;
    }

    public static boolean isValueInvalid(ByteString value) {
        return value.size() == 0 || value.size() > 10000;
    }

    public static KVResponse.Builder parseRequest(KVRequest kvRequest, long size) {
        KVResponse.Builder kvResponse = KVResponse.newBuilder();
        kvResponse.setErrCode(Utils.SUCCESS);

        if (kvRequest.getCommand() > 12) {
            kvResponse.setErrCode(Utils.UNRECOGNIZED_ERROR);
            return kvResponse;
        }

        if (kvRequest.hasKey() && isKeyInvalid(kvRequest.getKey())) {
            kvResponse.setErrCode(Utils.INVALID_KEY_ERROR);
            return kvResponse;
        }

        if (kvRequest.hasValue() && isValueInvalid(kvRequest.getValue())) {
            kvResponse.setErrCode(Utils.INVALID_VALUE_ERROR);
            return kvResponse;
        }

        if (Utils.isChangeRequest(kvRequest.getCommand()) || Utils.isReplicaRequest(kvRequest.getCommand())) {
            if (Utils.isOutOfMemory(Utils.UPPER_MIN_MEMORY) && size > Utils.MAX_CACHE_SIZE) {
                kvResponse.setErrCode(Utils.OVERLOAD_ERROR);
                kvResponse.setOverloadWaitTime(Utils.OVERLOAD_TIME);
                return kvResponse;
            } else if (Utils.isOutOfMemory(Utils.LOWER_MIN_MEMORY)) {
                kvResponse.setErrCode(Utils.MEMORY_ERROR);
                return kvResponse;
            }
        }

        return kvResponse;
    }

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

    public static boolean isCheckSumInvalid(Msg msg) {
        return msg.getCheckSum() != createCheckSum(msg.getMessageID().toByteArray(), msg.getPayload().toByteArray());
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

    public static int searchAddresses(int id, ConcurrentSkipListMap<Integer, Integer> addresses, List<Integer> replicas) {
        for (int nodeID : addresses.tailMap(id).keySet()) {
            int node = addresses.get(nodeID);
            if (!replicas.contains(node)) return node;
        }

        for (int nodeID : addresses.headMap(id).keySet()) {
            int node = addresses.get(nodeID);
            if (!replicas.contains(node)) return node;
        }

        return -1;
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

        if (addresses.size() != 0) addresses.put((int) Math.pow(2, M_BITS), addresses.get(addresses.firstKey()));
        return addresses;
    }

    public static SortedMap<Integer, ArrayList<Integer>> generateReplicas(ConcurrentSkipListMap<Integer, Integer> addresses) {
        SortedMap<Integer, ArrayList<Integer>> map = new TreeMap<>();

        for (int nodeID : addresses.keySet()) {
            ArrayList<Integer> replicas = new ArrayList<>();
            for (int i = 0; i < Utils.REPLICATION_FACTOR; i++) {
                int node = Utils.searchAddresses(nodeID, addresses, replicas);
                if (node != -1) replicas.add(node);
            }
            map.put(nodeID, replicas);
        }

        return map;
    }

    public static boolean isDeadNode(long time, int size) {
        long threshold = (long) Math.ceil(Utils.EPIDEMIC_TIMEOUT + Utils.EPIDEMIC_PERIOD * ((Math.log(size) / Math.log(2)) + Utils.EPIDEMIC_BUFFER));
        return System.currentTimeMillis() - time > threshold;
    }

    public static boolean isDeadQueue(long time) {
        return System.currentTimeMillis() - time > Utils.QUEUE_EXPIRATION;
    }

    public static int hashKey(ByteString key) {
        CRC32 crc = new CRC32();
        crc.update(key.toByteArray());
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