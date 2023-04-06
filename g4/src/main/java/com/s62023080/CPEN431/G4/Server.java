package com.s62023080.CPEN431.G4;

import ca.NetSysLab.ProtocolBuffers.KeyValueRequest.KVRequest;
import ca.NetSysLab.ProtocolBuffers.KeyValueResponse.KVResponse;
import ca.NetSysLab.ProtocolBuffers.Message.Msg;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.protobuf.ByteString;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.net.*;
import java.util.concurrent.*;

public class Server {
    private boolean running;
    private final int pid;
    private final int node;
    private final int weight;
    private final ArrayList<Integer> ports;
    private final Logger logger;
    private final DatagramSocket socket;
    private final ExecutorService executor;
    private final Store store;
    private final ReentrantReadWriteLock lock;
    private final ReentrantReadWriteLock tableLock;
    private final ReentrantReadWriteLock queueLock;
    private final Cache<ByteString, byte[]> cache;
    private final ConcurrentHashMap<ByteString, Integer> queue;
    private final ConcurrentHashMap<Integer, Long> nodes;
    private ConcurrentSkipListMap<Integer, Integer> addresses;

    public Server(ArrayList<Integer> nodes, int node, int threads, int weight) throws IOException {
        this.running = true;
        this.pid = (int) ProcessHandle.current().pid();
        this.node = node;
        this.weight = weight;
        this.ports = nodes;
        this.logger = new Logger(this.pid, this.node);
        this.socket = new DatagramSocket(this.node);
        this.executor = Executors.newFixedThreadPool(threads);
        this.store = new Store();
        this.lock = new ReentrantReadWriteLock();
        this.tableLock = new ReentrantReadWriteLock();
        this.queueLock = new ReentrantReadWriteLock();
        this.cache = CacheBuilder.newBuilder().expireAfterWrite(Utils.CACHE_EXPIRATION, TimeUnit.MILLISECONDS).build();
        this.queue = new ConcurrentHashMap<>();
        this.nodes = new ConcurrentHashMap<>();
        for (int n : nodes) this.nodes.put(n, System.currentTimeMillis());
        this.regen();

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
        scheduler.scheduleAtFixedRate(this::push, 0, Utils.EPIDEMIC_PERIOD, TimeUnit.MILLISECONDS);
        scheduler.scheduleAtFixedRate(this::pop, 0, Utils.POP_PERIOD, TimeUnit.MILLISECONDS);

        while (this.running) {
            try {
                DatagramPacket packet = new DatagramPacket(new byte[Utils.MAX_REQUEST_SIZE], Utils.MAX_REQUEST_SIZE);
                socket.receive(packet);
                Request request = new Request(packet);
                this.executor.submit(() -> this.handleRequest(request));
            } catch (Exception e) {
                this.logger.log(e.getMessage());
            }
        }
    }

    /**
     * REQUEST HANDLING
     */

    public void sendEpidemic(int command, int port) {
        try {
            KVRequest.Builder kvRequest = KVRequest.newBuilder();
            kvRequest.setCommand(command);
            kvRequest.putAllNodes(this.nodes);
            this.send(Utils.generateMessageID(this.node), kvRequest.build().toByteString(), InetAddress.getLocalHost(), port, false);
        } catch (UnknownHostException e) {
            this.logger.log(e.getMessage());
        }
    }

    public void sendConfirm(ByteString messageID, int port) {
        try {
            KVRequest.Builder kvRequest = KVRequest.newBuilder();
            kvRequest.setCommand(Utils.REPLICA_CONFIRMED);
            this.send(messageID, kvRequest.build().toByteString(), InetAddress.getLocalHost(), port, false);
        } catch (UnknownHostException e) {
            this.logger.log(e.getMessage());
        }
    }

    public void sendKey(ByteString key, Data data, int port) {
        this.queueLock.writeLock().lock();
        try {
            KVRequest.Builder kvRequest = KVRequest.newBuilder();
            kvRequest.setCommand(Utils.REPLICA_PUSH);
            kvRequest.setKey(key);
            kvRequest.setValue(data.value);
            kvRequest.setVersion(data.version);
            ByteString messageID = Utils.generateMessageID(this.node);
            this.send(messageID, kvRequest.build().toByteString(), InetAddress.getLocalHost(), port, true);
            this.queue.put(messageID, port);
        } catch (UnknownHostException e) {
            this.logger.log(e.getMessage());
        } finally {
            this.queueLock.writeLock().unlock();
        }
    }

    public void pop() {
        if (this.queue.size() == 0) return;

        this.queueLock.writeLock().lock();
        try {
            this.queue.keySet().removeIf(messageID -> this.cache.getIfPresent(messageID) == null);
            for (ByteString messageID : this.queue.keySet()) {
                byte[] cacheValue = this.cache.getIfPresent(messageID);
                if (cacheValue == null) continue;
                this.socket.send(new DatagramPacket(cacheValue, cacheValue.length, InetAddress.getLocalHost(), this.queue.get(messageID)));
                this.logger.log("Send key: " + this.queue.get(messageID));
            }
        } catch (IOException e) {
            this.logger.log(e.getMessage());
        } finally {
            this.queueLock.writeLock().unlock();
        }
    }

    public void redirect(Msg msg, KVRequest kvRequest, int node, List<Integer> replicas, InetAddress address, int port) {
        try {
            KVRequest.Builder reqClone = KVRequest.newBuilder();
            reqClone.addAllReplicas(replicas);
            if (kvRequest.hasKey()) reqClone.setKey(kvRequest.getKey());
            if (kvRequest.hasValue()) reqClone.setValue(kvRequest.getValue());
            if (kvRequest.hasVersion()) reqClone.setVersion(kvRequest.getVersion());
            Msg.Builder msgClone = Msg.newBuilder();
            msgClone.setMessageID(msg.getMessageID());
            msgClone.setPayload(reqClone.build().toByteString());
            msgClone.setCheckSum(msg.getCheckSum());
            msgClone.setAddress(ByteString.copyFrom(address.getAddress()));
            msgClone.setPort(port);
            byte[] response = msgClone.build().toByteArray();
            this.socket.send(new DatagramPacket(response, response.length, InetAddress.getLocalHost(), node));
        } catch (IOException e) {
            this.logger.log(e.getMessage());
        }
    }

    public void send(ByteString messageID, ByteString payload, InetAddress address, int port, boolean cache) {
        try {
            Msg.Builder msg = Msg.newBuilder();
            msg.setMessageID(messageID);
            msg.setPayload(payload);
            msg.setCheckSum(Utils.createCheckSum(messageID.toByteArray(), payload.toByteArray()));
            byte[] response = msg.build().toByteArray();
            this.socket.send(new DatagramPacket(response, response.length, address, port));
            if (cache) this.cache.put(msg.getMessageID(), response);
        } catch (IOException e) {
            this.logger.log(e.getMessage());
        }
    }

    public void handleRequest(Request request) {
        Msg msg = null;
        KVResponse.Builder kvResponse = null;

        try {
            msg = Msg.parseFrom(request.data);
            if (Utils.isCheckSumInvalid(msg)) return;
            if (msg.hasAddress()) request.address = InetAddress.getByAddress(msg.getAddress().toByteArray());
            if (msg.hasPort()) request.port = msg.getPort();

            // Parse request
            KVRequest kvRequest = KVRequest.parseFrom(msg.getPayload());
            kvResponse = Utils.parseRequest(kvRequest, this.cache.size());
            if (kvResponse.getErrCode() != 0) {
                this.send(msg.getMessageID(), kvResponse.build().toByteString(), request.address, request.port, true);
                return;
            }

            // Node request
            if (Utils.isNodeRequest(kvRequest.getCommand())) {
                switch (kvRequest.getCommand()) {
                    case Utils.SHUTDOWN_REQUEST -> System.exit(0);
                    case Utils.CLEAR_REQUEST -> {
                        this.store.clear();
                        this.cache.invalidateAll();
                    }
                    case Utils.HEALTH_REQUEST -> kvResponse.setErrCode(Utils.SUCCESS);
                    case Utils.PID_REQUEST -> kvResponse.setPid(this.pid);
                    case Utils.MEMBERSHIP_REQUEST -> kvResponse.setMembershipCount(this.nodes.size());
                }

                this.send(msg.getMessageID(), kvResponse.build().toByteString(), request.address, request.port, true);
                return;
            }

            // Epidemic request
            if (Utils.isEpidemicRequest(kvRequest.getCommand())) {
                switch (kvRequest.getCommand()) {
                    case Utils.EPIDEMIC_PUSH -> {
                        this.merge(kvRequest.getNodesMap());
                        this.sendEpidemic(Utils.EPIDEMIC_PULL, request.port);
                    }
                    case Utils.EPIDEMIC_PULL -> this.merge(kvRequest.getNodesMap());
                }

                return;
            }

            // Replica request
            if (Utils.isReplicaRequest(kvRequest.getCommand())) {
                switch (kvRequest.getCommand()) {
                    case Utils.REPLICA_PUSH -> {
                        this.store.put(kvRequest.getKey(), kvRequest.getValue(), kvRequest.getVersion());
                        this.sendConfirm(msg.getMessageID(), request.port);
                    }
                    case Utils.REPLICA_CONFIRMED -> {
                        this.queueLock.writeLock().lock();
                        try {
                            this.queue.remove(msg.getMessageID());
                        } finally {
                            this.queueLock.writeLock().unlock();
                        }
                    }
                }

                return;
            }

            // Get request
            if (kvRequest.getCommand() == Utils.GET_REQUEST) {
                List<Integer> replicas = new ArrayList<>();
                for (int i = 0; i < Utils.REPLICATION_FACTOR; i++) replicas.add(Utils.searchAddresses(kvRequest.getKey(), this.addresses, replicas));
                int node = replicas.get(replicas.size() - 1);

                if (node == this.node || (kvRequest.getReplicasList().contains(this.node) && kvRequest.getReplicasList().size() >= Utils.REPLICATION_FACTOR)) {
                    Data data = this.store.get(kvRequest.getKey());
                    if (data == null) kvResponse.setErrCode(Utils.MISSING_KEY_ERROR);
                    else {
                        kvResponse.setValue(data.value);
                        kvResponse.setVersion(data.version);
                    }
                    this.send(msg.getMessageID(), kvResponse.build().toByteString(), request.address, request.port, true);
                    return;
                }

                this.redirect(msg, kvRequest, node, replicas, request.address, request.port);
                return;
            }

            // Change request
            if (Utils.isChangeRequest(kvRequest.getCommand())) {
                List<Integer> replicas = kvRequest.getReplicasList();
                if (replicas.size() < Utils.REPLICATION_FACTOR) replicas.add(Utils.searchAddresses(kvRequest.getKey(), this.addresses, replicas));

                if (kvRequest.getReplicasList().contains(this.node)) {
                    if (kvRequest.getCommand() == Utils.PUT_REQUEST) {
                        this.store.put(kvRequest.getKey(), kvRequest.getValue(), kvRequest.getVersion());
                    } else if (kvRequest.getCommand() == Utils.REMOVE_REQUEST) {
                        Data data = this.store.remove(kvRequest.getKey());
                        if (data == null) kvResponse.setErrCode(Utils.MISSING_KEY_ERROR);
                    }

                    if (replicas.size() >= Utils.REPLICATION_FACTOR) {
                        this.send(msg.getMessageID(), kvResponse.build().toByteString(), request.address, request.port, true);
                        return;
                    }
                }

                int node = replicas.get(replicas.size() - 1);
                if (node == this.node) {
                    node = Utils.searchAddresses(kvRequest.getKey(), this.addresses, replicas);
                    replicas.add(node);
                }

                this.redirect(msg, kvRequest, node, replicas, request.address, request.port);
            }
        } catch (OutOfMemoryError e) {
            this.logger.log(e.getMessage(), Utils.getFreeMemory());
            if (msg != null && kvResponse != null)  {
                kvResponse.setErrCode(Utils.MEMORY_ERROR);
                this.send(msg.getMessageID(), kvResponse.build().toByteString(), request.address, request.port, true);
            }
        } catch (Exception e) {
            this.logger.log(e.getMessage(), Utils.getFreeMemory());
            if (msg != null && kvResponse != null)  {
                kvResponse.setErrCode(Utils.STORE_ERROR);
                this.send(msg.getMessageID(), kvResponse.build().toByteString(), request.address, request.port, true);
            }
        }
    }

    /**
     * EPIDEMIC PROTOCOL
     */

    public void regen() {
        this.tableLock.writeLock().lock();
        try {
            this.addresses = Utils.generateAddresses(new ArrayList<>(this.nodes.keySet()), this.weight);
            this.logger.logAddresses(this.addresses);
        } finally {
            this.tableLock.writeLock().unlock();
        }
    }

    public void push() {
        this.lock.writeLock().lock();
        try {
            this.nodes.put(this.node, System.currentTimeMillis());
            boolean regen = this.nodes.values().removeIf(value -> Utils.isDeadNode(value, this.nodes.size()));
            if (regen) this.regen();
            int port = this.ports.get(ThreadLocalRandom.current().nextInt(this.ports.size()));
            if (port == this.node) port = this.ports.get((port + 1) % this.ports.size());
            this.sendEpidemic(Utils.EPIDEMIC_PUSH, port);
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    public void merge(Map<Integer, Long> nodes) {
        ArrayList<Integer> joined = new ArrayList<>();

        this.lock.writeLock().lock();
        try {
            for (int node : nodes.keySet()) {
                if (node == this.node || Utils.isDeadNode(nodes.get(node), this.nodes.size())) continue;
                if (this.nodes.containsKey(node)) this.nodes.put(node, Math.max(this.nodes.get(node), nodes.get(node)));
                else {
                    this.nodes.put(node, nodes.get(node));
                    joined.add(node);
                    this.logger.log("Node Join: " + node);
                }
            }
        } finally {
            this.lock.writeLock().unlock();
        }

        this.join(joined);
    }

    public void join(ArrayList<Integer> nodes) {
        if (nodes.size() == 0) return;
        ArrayList<ByteString> keys = new ArrayList<>();

        this.lock.writeLock().lock();
        try {
            this.regen();
            // Continue
            for (int node : nodes) {
                for (ByteString key : this.store.getKeys()) {
                    Data data = this.store.get(key);
                    if (data == null) continue;
                    this.sendKey(key, data, node);
                    keys.add(key);
                    this.logger.log("Send Key: " + key + " => " + node);
                }
            }
        } finally {
            this.lock.writeLock().unlock();
        }

        this.store.bulkRemove(keys);
    }

    /**
     * Test Methods
     */

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
