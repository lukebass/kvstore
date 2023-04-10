package com.s62023080.CPEN431.G4;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentSkipListMap;

public class Logger {
    private final int pid;

    public Logger(int pid) {
        this.pid = pid;
    }

    public void log(String message) {
        System.out.println(message);
        System.out.println("Log: " + System.currentTimeMillis());
        System.out.println("PID: " + pid);
        System.out.println("\n");
    }

    public void log(String message, int memory, long cache) {
        System.out.println(message);
        System.out.println("Memory: " + memory);
        System.out.println("Cache: " + cache);
        System.out.println("Log: " + System.currentTimeMillis());
        System.out.println("PID: " + pid);
        System.out.println("\n");
    }

    public void log(String message, ArrayList<Integer> replicas) {
        System.out.println(message);
        System.out.println("Replicas: " + replicas.toString());
        System.out.println("Log: " + System.currentTimeMillis());
        System.out.println("PID: " + pid);
        System.out.println("\n");
    }

    public void log(ConcurrentSkipListMap<Integer, Integer> addresses) {
        System.out.println("Addresses Size: " + addresses.size());
        System.out.println(addresses.keySet());
        System.out.println(addresses.values());
        System.out.println("\n");
    }
}
