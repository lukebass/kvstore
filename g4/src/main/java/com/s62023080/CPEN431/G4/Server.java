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
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.net.*;
import java.util.concurrent.*;
import java.util.*;

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
    private final Cache<ByteString, CacheData> cache;
    private final ConcurrentHashMap<ByteString, CacheData> queue;
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
        this.addresses = Utils.generateAddresses(new ArrayList<>(this.nodes.keySet()), this.weight);
        this.logger.log(this.addresses);

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
        scheduler.scheduleAtFixedRate(this::push, 0, Utils.EPIDEMIC_PERIOD, TimeUnit.MILLISECONDS);
        scheduler.scheduleAtFixedRate(this::pop, 250, Utils.POP_PERIOD, TimeUnit.MILLISECONDS);

        while (this.running) {
            try {
                DatagramPacket packet = new DatagramPacket(new byte[Utils.MAX_REQUEST_SIZE], Utils.MAX_REQUEST_SIZE);
                socket.receive(packet);
                Request request = new Request(packet);
                this.executor.submit(() -> this.handleRequest(request));
            } catch (Exception e) {
                this.logger.log("Error : " + e.getMessage());
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
            this.logger.log("Epidemic " + e.getMessage());
        }
    }

    public void sendConfirm(ByteString messageID, int port) {
        try {
            KVRequest.Builder kvRequest = KVRequest.newBuilder();
            kvRequest.setCommand(Utils.REPLICA_CONFIRMED);
            this.send(messageID, kvRequest.build().toByteString(), InetAddress.getLocalHost(), port, true);
        } catch (UnknownHostException e) {
            this.logger.log("Confirm " + e.getMessage());
        }
    }

    public void receiveConfirm(ByteString messageID) {
        this.queueLock.writeLock().lock();
        try {
            this.queue.remove(messageID);
            this.cache.put(messageID, new CacheData(null, null, 0));
        } finally {
            this.queueLock.writeLock().unlock();
        }
    }

    public void sendReplica(ByteString key, Data data, int port) {
        this.queueLock.writeLock().lock();
        try {
            KVRequest.Builder kvRequest = KVRequest.newBuilder();
            kvRequest.setCommand(Utils.REPLICA_PUSH);
            kvRequest.setKey(key);
            kvRequest.setValue(data.value);
            kvRequest.setVersion(data.version);
            ByteString messageID = Utils.generateMessageID(this.node);
            CacheData cached = this.send(messageID, kvRequest.build().toByteString(), InetAddress.getLocalHost(), port, false);
            if (cached != null) this.queue.put(messageID, cached);
        } catch (UnknownHostException e) {
            this.logger.log("Replica " + e.getMessage());
        } finally {
            this.queueLock.writeLock().unlock();
        }
    }

    public void redirect(Msg msg, KVRequest kvRequest, int node, ArrayList<Integer> replicas, InetAddress address, int port, boolean cache) {
        if (node == -1) return;
        try {
            KVRequest.Builder reqClone = KVRequest.newBuilder();
            reqClone.setCommand(kvRequest.getCommand());
            reqClone.addAllReplicas(replicas);
            if (kvRequest.hasKey()) reqClone.setKey(kvRequest.getKey());
            if (kvRequest.hasValue()) reqClone.setValue(kvRequest.getValue());
            if (kvRequest.hasVersion()) reqClone.setVersion(kvRequest.getVersion());
            ByteString payload = reqClone.build().toByteString();
            Msg.Builder msgClone = Msg.newBuilder();
            msgClone.setMessageID(msg.getMessageID());
            msgClone.setPayload(payload);
            msgClone.setCheckSum(Utils.createCheckSum(msg.getMessageID().toByteArray(), payload.toByteArray()));
            msgClone.setAddress(ByteString.copyFrom(address.getAddress()));
            msgClone.setPort(port);
            byte[] response = msgClone.build().toByteArray();
            this.socket.send(new DatagramPacket(response, response.length, InetAddress.getLocalHost(), node));
            if (cache) this.cache.put(msg.getMessageID(), new CacheData(response, InetAddress.getLocalHost(), node));
        } catch (IOException e) {
            this.logger.log("Redirect " + e.getMessage());
        }
    }

    public CacheData send(ByteString messageID, ByteString payload, InetAddress address, int port, boolean cache) {
        try {
            Msg.Builder msg = Msg.newBuilder();
            msg.setMessageID(messageID);
            msg.setPayload(payload);
            msg.setCheckSum(Utils.createCheckSum(messageID.toByteArray(), payload.toByteArray()));
            byte[] response = msg.build().toByteArray();
            this.socket.send(new DatagramPacket(response, response.length, address, port));
            CacheData cached = new CacheData(response, address, port);
            if (cache) this.cache.put(msg.getMessageID(), cached);
            return cached;
        } catch (IOException e) {
            this.logger.log("Send " + e.getMessage());
        }
        return null;
    }

    public void pop() {
        if (this.queue.size() == 0) return;
        this.queueLock.writeLock().lock();
        try {
            this.queue.values().removeIf(cached -> Utils.isDeadQueue(cached.time));
            for (ByteString messageID : this.queue.keySet()) {
                CacheData cached = this.queue.get(messageID);
                if (cached == null) continue;
                this.socket.send(new DatagramPacket(cached.data, cached.data.length, cached.address, cached.port));
            }
        } catch (IOException e) {
            this.logger.log("Pop " + e.getMessage());
        } finally {
            this.queueLock.writeLock().unlock();
        }
    }

    public boolean check(ByteString messageID, boolean send) {
        try {
            CacheData cached = this.cache.getIfPresent(messageID);
            if (cached == null) return false;
            if (send) this.socket.send(new DatagramPacket(cached.data, cached.data.length, cached.address, cached.port));
            return true;
        } catch (IOException e) {
            this.logger.log("Check " + e.getMessage());
        }
        return false;
    }

    public void handleRequest(Request request) {
        Msg msg = null;
        KVResponse.Builder kvResponse = null;

        try {
            msg = Msg.parseFrom(request.data);
            if (Utils.isCheckSumInvalid(msg)) {
                this.logger.log("CheckSum: " + msg.getMessageID());
                return;
            }
            if (msg.hasAddress()) request.address = InetAddress.getByAddress(msg.getAddress().toByteArray());
            if (msg.hasPort()) request.port = msg.getPort();

            KVRequest kvRequest = KVRequest.parseFrom(msg.getPayload());

            // Cache check
            if (this.check(msg.getMessageID(), !Utils.isReplicaRequest(kvRequest.getCommand()))) return;

            // Parse request
            kvResponse = Utils.parseRequest(kvRequest);
            if (kvResponse.getErrCode() != 0) {
                this.logger.log("Parse: " + kvResponse.getErrCode());
                this.send(msg.getMessageID(), kvResponse.build().toByteString(), request.address, request.port, false);
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
            }

            // Replica request
            if (Utils.isReplicaRequest(kvRequest.getCommand())) {
                switch (kvRequest.getCommand()) {
                    case Utils.REPLICA_PUSH -> {
                        if (Utils.memoryCheck(this.cache.size()) != null) return;
                        this.store.put(kvRequest.getKey(), kvRequest.getValue(), kvRequest.getVersion());
                        this.sendConfirm(msg.getMessageID(), request.port);
                    }
                    case Utils.REPLICA_CONFIRMED -> this.receiveConfirm(msg.getMessageID());
                }
            }

            // Get request
            if (kvRequest.getCommand() == Utils.GET_REQUEST) {
                ArrayList<Integer> replicas = new ArrayList<>();
                for (int i = 0; i < Utils.REPLICATION_FACTOR; i++) replicas.add(Utils.searchAddresses(Utils.hashKey(kvRequest.getKey()), this.addresses, replicas));
                int node = replicas.get(replicas.size() - 1);

                if (node != this.node) {
                    this.redirect(msg, kvRequest, node, replicas, request.address, request.port, false);
                    return;
                }

                Data data = this.store.get(kvRequest.getKey());
                if (data == null) kvResponse.setErrCode(Utils.MISSING_KEY_ERROR);
                else {
                    kvResponse.setValue(data.value);
                    kvResponse.setVersion(data.version);
                }

                this.send(msg.getMessageID(), kvResponse.build().toByteString(), request.address, request.port, true);
            }

            // Change request
            if (Utils.isChangeRequest(kvRequest.getCommand())) {
                ArrayList<Integer> replicas = new ArrayList<>(kvRequest.getReplicasList());
                if (replicas.size() < Utils.REPLICATION_FACTOR) replicas.add(Utils.searchAddresses(Utils.hashKey(kvRequest.getKey()), this.addresses, replicas));
                int node = replicas.get(replicas.size() - 1);

                if (replicas.contains(this.node)) {
                    ByteString payload = Utils.memoryCheck(this.cache.size());
                    if (payload != null) {
                        this.send(msg.getMessageID(), payload, request.address, request.port, false);
                        this.logger.log("Memory: " + kvResponse.getErrCode(), Utils.getFreeMemory());
                        return;
                    }

                    if (kvRequest.getCommand() == Utils.PUT_REQUEST) {
                        this.store.put(kvRequest.getKey(), kvRequest.getValue(), kvRequest.getVersion());
                    } else if (kvRequest.getCommand() == Utils.REMOVE_REQUEST) {
                        Data data = this.store.remove(kvRequest.getKey());
                        if (data == null) kvResponse.setErrCode(Utils.MISSING_KEY_ERROR);
                    }

                    if (node == this.node) {
                        if (replicas.size() == Utils.REPLICATION_FACTOR) {
                            this.send(msg.getMessageID(), kvResponse.build().toByteString(), request.address, request.port, true);
                            return;
                        }

                        node = Utils.searchAddresses(Utils.hashKey(kvRequest.getKey()), this.addresses, replicas);
                        replicas.add(node);
                    }
                }

                this.redirect(msg, kvRequest, node, replicas, request.address, request.port, replicas.contains(this.node));
            }
        } catch (Exception e) {
            this.logger.log("Request: " + e.getMessage(), Utils.getFreeMemory());
            if (msg != null && kvResponse != null)  {
                kvResponse.setErrCode(Utils.STORE_ERROR);
                this.send(msg.getMessageID(), kvResponse.build().toByteString(), request.address, request.port, false);
            }
        }
    }

    /**
     * EPIDEMIC PROTOCOL
     */

    public void regen() {
        SortedMap<Integer, ArrayList<Integer>> oldMap;
        SortedMap<Integer, ArrayList<Integer>> newMap;

        this.tableLock.writeLock().lock();
        try {
            oldMap = Utils.generateReplicas(this.addresses);
            this.addresses = Utils.generateAddresses(new ArrayList<>(this.nodes.keySet()), this.weight);
            newMap = Utils.generateReplicas(this.addresses);
            this.logger.log(this.addresses);
        } finally {
            this.tableLock.writeLock().unlock();
        }

        for (ByteString key : this.store.getKeys()) {
            ArrayList<Integer> oldReplicas = oldMap.get(oldMap.tailMap(Utils.hashKey(key)).firstKey());
            ArrayList<Integer> newReplicas = newMap.get(newMap.tailMap(Utils.hashKey(key)).firstKey());

            for (int node : newReplicas) {
                if (!oldReplicas.contains(node)) {
                    Data data = this.store.get(key);
                    if (data == null) continue;
                    this.logger.log("Send Replica: " + node);
                    this.sendReplica(key, data, node);
                }
            }

            if (!newReplicas.contains(this.node)) this.store.remove(key);
        }
    }

    public void broadcast() {
        this.logger.log("Node Alive");
        this.nodes.put(this.node, System.currentTimeMillis());
        for (int port : this.ports) {
            if (port == this.node) continue;
            this.sendEpidemic(Utils.EPIDEMIC_PUSH, port);
        }
    }

    public void push() {
        boolean regen;

        this.lock.writeLock().lock();
        try {
            if (System.currentTimeMillis() - this.nodes.get(this.node) > Utils.EPIDEMIC_TIMEOUT) {
                this.broadcast();
                return;
            }

            this.nodes.put(this.node, System.currentTimeMillis());
            regen = this.nodes.values().removeIf(value -> Utils.isDeadNode(value, this.nodes.size()));
            ArrayList<Integer> nodes = new ArrayList<>(this.nodes.keySet());
            int random = ThreadLocalRandom.current().nextInt(nodes.size());
            int port = nodes.get(random);
            if (port == this.node) port = nodes.get((random + 1) % nodes.size());
            if (port != this.node) this.sendEpidemic(Utils.EPIDEMIC_PUSH, port);
        } finally {
            this.lock.writeLock().unlock();
        }

        if (regen) {
            this.logger.log("Node Leave");
            this.regen();
        }
    }

    public void merge(Map<Integer, Long> nodes) {
        boolean regen = false;

        this.lock.writeLock().lock();
        try {
            for (int node : nodes.keySet()) {
                if (node == this.node || Utils.isDeadNode(nodes.get(node), this.nodes.size())) continue;
                if (this.nodes.containsKey(node)) this.nodes.put(node, Math.max(this.nodes.get(node), nodes.get(node)));
                else {
                    regen = true;
                    this.nodes.put(node, nodes.get(node));
                    this.logger.log("Node Join: " + node);
                }
            }
        } finally {
            this.lock.writeLock().unlock();
        }

        if (regen) this.regen();
    }

    /**
     * Test Methods
     */

    public Store getStore() {
        return this.store;
    }

    public Cache<ByteString, CacheData> getCache() {
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
