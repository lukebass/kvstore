package com.s62023080.CPEN431.A4;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.io.IOException;
import java.net.*;
import java.util.concurrent.*;

public class Server {
    private final ExecutorService executor;

    private final Store store;

    private final Cache<Key, byte[]> cache;

    private boolean running;

    public Server(int port, int nThreads, int cacheExpiration, int waitTime) throws IOException {
        DatagramSocket socket = new DatagramSocket(port);
        this.executor = Executors.newFixedThreadPool(nThreads);
        this.store = new Store();
        this.cache = CacheBuilder.newBuilder().expireAfterWrite(cacheExpiration, TimeUnit.MILLISECONDS).build();
        this.running = true;

        while (this.running) {
            try {
                DatagramPacket packet = new DatagramPacket(new byte[Utils.MAX_REQUEST_SIZE], Utils.MAX_REQUEST_SIZE);
                socket.receive(packet);
                this.executor.submit(new ServerResponse(socket, packet, this.store, this.cache, waitTime));
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
    }

    public void shutdown() {
        this.running = false;
        this.executor.shutdown();
        this.clear();
    }
}
