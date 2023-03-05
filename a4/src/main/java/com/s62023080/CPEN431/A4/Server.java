package com.s62023080.CPEN431.A4;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.net.*;
import java.util.concurrent.*;

public class Server {
    private final ExecutorService executor;

    private final Store store;

    private final Cache<Key, byte[]> cache;

    private final ConcurrentSkipListMap<Integer, Integer> addresses;

    private final ConcurrentSkipListMap<Integer, int[]> tables;

    private boolean running;

    public Server(ArrayList<Integer> nodes, int port, int threads, int weight) throws IOException {
        DatagramSocket socket = new DatagramSocket(port);
        this.executor = Executors.newFixedThreadPool(threads);
        this.store = new Store();
        this.cache = CacheBuilder.newBuilder().expireAfterWrite(Utils.CACHE_EXPIRATION, TimeUnit.MILLISECONDS).build();
        this.addresses = Utils.generateAddresses(nodes, weight);
        this.tables = Utils.generateTables(port, weight, new ArrayList<>(addresses.keySet()));
        this.running = true;

        for (int node : tables.keySet()) {
            System.out.println("Node: " + node);
            for (int table : tables.get(node)) System.out.println(table);
        }

        while (this.running) {
            try {
                DatagramPacket packet = new DatagramPacket(new byte[Utils.MAX_REQUEST_SIZE], Utils.MAX_REQUEST_SIZE);
                socket.receive(packet);
                this.executor.submit(new ServerResponse(socket, packet, this.store, this.cache, this.addresses, this.tables));
                System.out.println(this.cache.size() + " / " + this.store.size() + " / " + Utils.getFreeMemory());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public Store getStore() {
        return this.store;
    }

    public Cache<Key, byte[]> getCache() {
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
