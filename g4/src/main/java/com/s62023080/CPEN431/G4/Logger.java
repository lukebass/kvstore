package com.s62023080.CPEN431.G4;

import java.util.concurrent.ConcurrentSkipListMap;

public class Logger {
    private final int pid;
    private final int port;

    public Logger(int pid, int port) {
        this.pid = pid;
        this.port = port;
    }

    public void log(String message) {
        System.out.println("Log: " + System.currentTimeMillis());
        System.out.println("PID: " + pid);
        System.out.println("Port: " + port);
        System.out.println(message);
    }

    public void log(String message, int memory) {
        System.out.println("Log: " + System.currentTimeMillis());
        System.out.println("PID: " + pid);
        System.out.println("Port: " + port);
        System.out.println("Memory: " + memory);
        System.out.println(message);
    }

    public void logAddresses(ConcurrentSkipListMap<Integer, Integer> addresses) {
        this.log("Table Size: " + addresses.size());
        for (int nodeID : addresses.keySet()) {
            System.out.println("Node ID: " + nodeID);
            System.out.println("Node Port: " + addresses.get(nodeID));
        }
    }
}
