package com.s62023080.CPEN431.G4;

import ca.NetSysLab.ProtocolBuffers.KeyValueRequest;
import ca.NetSysLab.ProtocolBuffers.KeyValueResponse;
import ca.NetSysLab.ProtocolBuffers.Message;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.protobuf.ByteString;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.net.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Server {
    private boolean running;
    private final int pid;
    private final int port;
    private final int weight;
    private final ArrayList<Integer> ports;
    private final DatagramSocket socket;
    private final ExecutorService executor;
    private final Store store;
    private final Cache<ByteString, byte[]> cache;
    private final ConcurrentHashMap<Integer, Long> nodes;
    private final ReentrantReadWriteLock lock;
    private ConcurrentSkipListMap<Integer, Integer> addresses;
    private ConcurrentSkipListMap<Integer, int[]> tables;

    public Server(ArrayList<Integer> nodes, int port, int threads, int weight) throws IOException {
        this.running = true;
        this.pid = (int) ProcessHandle.current().pid();
        this.port = port;
        this.weight = weight;
        this.ports = nodes;
        this.socket = new DatagramSocket(port);
        this.executor = Executors.newFixedThreadPool(threads);
        this.store = new Store();
        this.cache = CacheBuilder.newBuilder().expireAfterWrite(Utils.CACHE_EXPIRATION, TimeUnit.MILLISECONDS).build();
        this.nodes = new ConcurrentHashMap<>();
        this.lock = new ReentrantReadWriteLock();
        this.addresses = new ConcurrentSkipListMap<>();
        this.tables = new ConcurrentSkipListMap<>();
        for (int node : nodes) this.nodes.put(node, System.currentTimeMillis());
        this.generateTables();
        ScheduledExecutorService push = Executors.newScheduledThreadPool(1);
        push.scheduleAtFixedRate(this::push, 0, Utils.EPIDEMIC_PERIOD, TimeUnit.MILLISECONDS);

        while (this.running) {
            try {
                DatagramPacket packet = new DatagramPacket(new byte[Utils.MAX_REQUEST_SIZE], Utils.MAX_REQUEST_SIZE);
                socket.receive(packet);
                Request request = new Request(packet);
                this.executor.submit(() -> this.handleRequest(request));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public boolean isStoreRequest(int command) {
        return command == Utils.PUT_REQUEST || command == Utils.GET_REQUEST || command == Utils.REMOVE_REQUEST;
    }

    public boolean isEpidemicRequest(int command) {
        return command == Utils.EPIDEMIC_PUSH || command == Utils.EPIDEMIC_PULL;
    }

    public boolean isKeyInvalid(ByteString key) {
        return key.size() == 0 || key.size() > 32;
    }

    public boolean isValueInvalid(ByteString value) {
        return value.size() == 0 || value.size() > 10000;
    }

    public void sendEpidemic(int command, int port) {
        try {
            KeyValueRequest.KVRequest.Builder kvRequest = KeyValueRequest.KVRequest.newBuilder();
            kvRequest.setCommand(command);
            kvRequest.putAllNodes(this.nodes);
            this.send(ByteString.copyFrom(Utils.generateMessageID(this.port)), kvRequest.build().toByteString(), InetAddress.getLocalHost(), port, false);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    public void redirect(Message.Msg msg, int nodeID, InetAddress address, int port) {
        try {
            Message.Msg.Builder clone = Message.Msg.newBuilder();
            clone.setMessageID(msg.getMessageID());
            clone.setPayload(msg.getPayload());
            clone.setCheckSum(msg.getCheckSum());
            clone.setAddress(ByteString.copyFrom(address.getAddress()));
            clone.setPort(port);
            byte[] response = clone.build().toByteArray();
            this.socket.send(new DatagramPacket(response, response.length, InetAddress.getLocalHost(), this.addresses.get(nodeID)));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void send(ByteString messageID, ByteString payload, InetAddress address, int port, boolean cache) {
        try {
            Message.Msg.Builder msg = Message.Msg.newBuilder();
            msg.setMessageID(messageID);
            msg.setPayload(payload);
            msg.setCheckSum(Utils.createCheckSum(messageID.toByteArray(), payload.toByteArray()));
            byte[] response = msg.build().toByteArray();
            this.socket.send(new DatagramPacket(response, response.length, address, port));
            if (cache) this.cache.put(msg.getMessageID(), response);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void handleRequest(Request request) {
        Message.Msg msg = null;
        KeyValueResponse.KVResponse.Builder kvResponse = KeyValueResponse.KVResponse.newBuilder();

        try {
            msg = Message.Msg.parseFrom(request.data);
            if (msg.getAddress().size() != 0) request.address = InetAddress.getByAddress(msg.getAddress().toByteArray());
            if (msg.getPort() != 0) request.port = msg.getPort();
            KeyValueRequest.KVRequest kvRequest = KeyValueRequest.KVRequest.parseFrom(msg.getPayload());
            byte[] cacheValue = this.cache.getIfPresent(msg.getMessageID());

            if (Utils.isCheckSumInvalid(msg.getCheckSum(), msg.getMessageID().toByteArray(), msg.getPayload().toByteArray())){
                return;
            } else if (this.isStoreRequest(kvRequest.getCommand()) && !Utils.isLocalKey(kvRequest.getKey().toByteArray(), this.tables)) {
                this.redirect(msg, Utils.searchTables(kvRequest.getKey().toByteArray(), this.tables), request.address, request.port);
                return;
            } else if (this.isEpidemicRequest(kvRequest.getCommand())) {
                Map<Integer, Long> nodes = kvRequest.getNodesMap();
                for (int node : nodes.keySet()) {
                    if (node == this.port) continue;
                    if (this.nodes.containsKey(node)) this.nodes.put(node, Math.max(this.nodes.get(node), nodes.get(node)));
                    else this.join(node, nodes.get(node));
                }
                if (kvRequest.getCommand() == Utils.EPIDEMIC_PUSH) this.sendEpidemic(Utils.EPIDEMIC_PULL, request.port);
                return;
            } else if (cacheValue != null) {
                this.socket.send(new DatagramPacket(cacheValue, cacheValue.length, request.address, request.port));
                return;
            } else if (this.cache.size() > Utils.MAX_CACHE_SIZE && Utils.isOutOfMemory(Utils.UPPER_MIN_MEMORY)) {
                throw new IOException("Too many requests");
            } else if (Utils.isOutOfMemory(Utils.LOWER_MIN_MEMORY)) {
                throw new OutOfMemoryError("Out of memory");
            }

            switch (kvRequest.getCommand()) {
                case Utils.PUT_REQUEST -> {
                    if (this.isKeyInvalid(kvRequest.getKey())) {
                        kvResponse.setErrCode(Utils.INVALID_KEY_ERROR);
                    } else if (this.isValueInvalid(kvRequest.getValue())) {
                        kvResponse.setErrCode(Utils.INVALID_VALUE_ERROR);
                    } else {
                        this.store.put(kvRequest.getKey(), kvRequest.getValue(), kvRequest.getVersion());
                        kvResponse.setErrCode(Utils.SUCCESS);
                    }
                }
                case Utils.GET_REQUEST -> {
                    if (this.isKeyInvalid(kvRequest.getKey())) {
                        kvResponse.setErrCode(Utils.INVALID_KEY_ERROR);
                    } else {
                        Data data = this.store.get(kvRequest.getKey());
                        if (data == null) {
                            kvResponse.setErrCode(Utils.MISSING_KEY_ERROR);
                        } else {
                            kvResponse.setErrCode(Utils.SUCCESS);
                            kvResponse.setValue(data.value);
                            kvResponse.setVersion(data.version);
                        }
                    }
                }
                case Utils.REMOVE_REQUEST -> {
                    if (this.isKeyInvalid(kvRequest.getKey())) {
                        kvResponse.setErrCode(Utils.INVALID_KEY_ERROR);
                    } else {
                        Data data = this.store.remove(kvRequest.getKey());
                        if (data == null) kvResponse.setErrCode(Utils.MISSING_KEY_ERROR);
                        else kvResponse.setErrCode(Utils.SUCCESS);
                    }
                }
                case Utils.SHUTDOWN_REQUEST -> System.exit(0);
                case Utils.CLEAR_REQUEST -> {
                    kvResponse.setErrCode(Utils.SUCCESS);
                    this.store.clear();
                    this.cache.invalidateAll();
                }
                case Utils.HEALTH_REQUEST -> kvResponse.setErrCode(Utils.SUCCESS);
                case Utils.PID_REQUEST -> {
                    kvResponse.setErrCode(Utils.SUCCESS);
                    kvResponse.setPid(this.pid);
                }
                case Utils.MEMBERSHIP_REQUEST -> {
                    kvResponse.setErrCode(Utils.SUCCESS);
                    kvResponse.setMembershipCount(this.nodes.size());
                }
                default -> kvResponse.setErrCode(Utils.UNRECOGNIZED_ERROR);
            }
        } catch (IOException e) {
            System.out.println("Overload Error: " + Utils.getFreeMemory());
            kvResponse.setErrCode(Utils.OVERLOAD_ERROR);
            kvResponse.setOverloadWaitTime(Utils.OVERLOAD_TIME);
        } catch (OutOfMemoryError e) {
            System.out.println("Memory Error: " + Utils.getFreeMemory());
            kvResponse.setErrCode(Utils.MEMORY_ERROR);
        } catch (Exception e) {
            System.out.println("Store Error:");
            System.out.println(e.getMessage());
            e.printStackTrace();
            kvResponse.setErrCode(Utils.STORE_ERROR);
        }

        if (msg != null) this.send(msg.getMessageID(), kvResponse.build().toByteString(), request.address, request.port, true);
    }

    public void generateTables() {
        this.addresses = Utils.generateAddresses(new ArrayList<>(this.nodes.keySet()), this.weight);
        this.tables = Utils.generateTables(new ArrayList<>(this.addresses.keySet()), this.port, this.weight);
    }

    public void push() {
        this.lock.writeLock().lock();
        this.nodes.put(this.port, System.currentTimeMillis());
        boolean regen = this.nodes.values().removeIf(value -> System.currentTimeMillis() - value > Utils.calculateThreshold(this.nodes.size()));
        if (regen) this.generateTables();
        this.lock.writeLock().unlock();
        int port = this.ports.get(ThreadLocalRandom.current().nextInt(this.ports.size()));
        while (port == this.port) port = this.ports.get(ThreadLocalRandom.current().nextInt(this.ports.size()));
        this.sendEpidemic(Utils.EPIDEMIC_PUSH, port);
    }

    public void join(int node, long time) {
        this.lock.writeLock().lock();
        this.nodes.put(node, time);
        this.generateTables();
        this.lock.writeLock().unlock();
        ConcurrentSkipListMap<Integer, int[]> nodeTables = Utils.generateTables(new ArrayList<>(this.addresses.keySet()), node, this.weight);
        for (ByteString key : new HashSet<>(this.store.getKeys())) {
            if (Utils.isLocalKey(key.toByteArray(), nodeTables)) {
                try {
                    Data data = this.store.get(key);
                    KeyValueRequest.KVRequest.Builder kvRequest = KeyValueRequest.KVRequest.newBuilder();
                    kvRequest.setCommand(Utils.PUT_REQUEST);
                    kvRequest.setKey(key);
                    kvRequest.setValue(data.value);
                    kvRequest.setVersion(data.version);
                    this.send(ByteString.copyFrom(Utils.generateMessageID(this.port)), kvRequest.build().toByteString(), InetAddress.getLocalHost(), node, false);
                    this.store.remove(key);
                } catch (UnknownHostException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public Store getStore() {
        return this.store;
    }

    public Cache<ByteString, byte[]> getCache() {
        return this.cache;
    }

    public void clear() {
        this.store.clear();
        this.cache.invalidateAll();
        this.addresses.clear();
        this.tables.clear();
    }

    public void shutdown() {
        this.running = false;
        this.executor.shutdown();
        this.clear();
    }

    /**
     * @param args args[0] servers.txt; args[1] port; args[2] threads; args[3] weight
     */
    public static void main(String[] args) {
       ArrayList<Integer> nodes = new ArrayList<>();

        try {
            BufferedReader reader = new BufferedReader(new FileReader(args[0]));
            String line = reader.readLine();
            while (line != null) {
                nodes.add(Integer.parseInt(line.split(":")[1]));
                line = reader.readLine();
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }

        try {
            new Server(nodes, Integer.parseInt(args[1]), Integer.parseInt(args[2]), Integer.parseInt(args[3]));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
