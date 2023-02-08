package com.s62023080.CPEN431.A4;

import ca.NetSysLab.ProtocolBuffers.Message.Msg;
import ca.NetSysLab.ProtocolBuffers.KeyValueRequest.KVRequest;
import ca.NetSysLab.ProtocolBuffers.KeyValueResponse.KVResponse;
import com.google.common.cache.Cache;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.net.*;

public class ServerResponse implements Runnable {
    private final DatagramSocket socket;

    private final DatagramPacket packet;

    private final Store store;

    private final Cache<ByteString, byte[]> cache;

    private final int waitTime;

    private final static int SUCCESS = 0;

    private final static int MISSING_KEY_ERROR = 1;

    private final static int MEMORY_ERROR = 2;

    private final static int OVERLOAD_ERROR = 3;

    private final static int STORE_ERROR = 4;

    private final static int UNRECOGNIZED_ERROR = 5;

    private final static int INVALID_KEY_ERROR = 6;

    private final static int INVALID_VALUE_ERROR = 7;

    public ServerResponse(DatagramSocket socket, DatagramPacket packet, Store store, Cache<ByteString, byte[]> cache, int waitTime) {
        this.socket = socket;
        this.packet = packet;
        this.store = store;
        this.cache = cache;
        this.waitTime = waitTime;
    }

    public void run() {
        Msg.Builder resMsg = Msg.newBuilder();
        KVResponse.Builder kvResponse = KVResponse.newBuilder();

        try {
            ByteBuffer buffer = ByteBuffer.wrap(this.packet.getData());
            byte[] request = new byte[this.packet.getLength()];
            buffer.get(request);
            Msg reqMsg = Msg.parseFrom(request);
            resMsg.setMessageID(reqMsg.getMessageID());
            KVRequest kvRequest = KVRequest.parseFrom(reqMsg.getPayload());

            byte[] cacheValue = this.cache.getIfPresent(reqMsg.getMessageID());
            if (Utils.isCheckSumInvalid(reqMsg.getCheckSum(), reqMsg.getMessageID().toByteArray(), reqMsg.getPayload().toByteArray())) {
                return;
            } else if (cacheValue != null) {
                resMsg.setPayload(ByteString.copyFrom(cacheValue));
                resMsg.setCheckSum(Utils.createCheckSum(resMsg.getMessageID().toByteArray(), resMsg.getPayload().toByteArray()));
                byte[] response = resMsg.build().toByteArray();
                DatagramPacket resPacket = new DatagramPacket(response, response.length, this.packet.getAddress(), this.packet.getPort());
                this.socket.send(resPacket);
                return;
            } else if (this.cache.size() > 1000 && Utils.isOutOfMemory(Utils.MAX_REQUEST_SIZE * 200L)) {
                throw new IOException("Too many requests");
            } else if (kvRequest.getCommand() == 1 && Utils.isOutOfMemory(Utils.MAX_REQUEST_SIZE * 100L)) {
                throw new OutOfMemoryError("Out of memory");
            } else if (Utils.isOutOfMemory(Utils.MAX_REQUEST_SIZE * 100L)) {
                throw new IOException("Out of memory");
            }

            switch (kvRequest.getCommand()) {
                // Put
                case 1 -> {
                    if (kvRequest.getKey().size() == 0 || kvRequest.getKey().size() > 32) {
                        kvResponse.setErrCode(INVALID_KEY_ERROR);
                    } else if (kvRequest.getValue().size() == 0 || kvRequest.getValue().size() > 10000) {
                        kvResponse.setErrCode(INVALID_VALUE_ERROR);
                    } else {
                        this.store.put(kvRequest.getKey(), kvRequest.getValue().toByteArray(), kvRequest.getVersion());
                        kvResponse.setErrCode(SUCCESS);
                    }
                }
                // Get
                case 2 -> {
                    if (kvRequest.getKey().size() == 0 || kvRequest.getKey().size() > 32) {
                        kvResponse.setErrCode(INVALID_KEY_ERROR);
                    } else {
                        byte[] composite = this.store.get(kvRequest.getKey());
                        if (composite == null) {
                            kvResponse.setErrCode(MISSING_KEY_ERROR);
                        } else {
                            buffer = ByteBuffer.wrap(composite);
                            int version = buffer.getInt();
                            byte[] value = new byte[composite.length - 4];
                            buffer.get(value);
                            kvResponse.setErrCode(SUCCESS);
                            kvResponse.setValue(ByteString.copyFrom(value));
                            kvResponse.setVersion(version);
                        }
                    }
                }
                // Remove
                case 3 -> {
                    if (kvRequest.getKey().size() == 0 || kvRequest.getKey().size() > 32) {
                        kvResponse.setErrCode(INVALID_KEY_ERROR);
                    } else {
                        byte[] composite = this.store.remove(kvRequest.getKey());
                        if (composite == null) {
                            kvResponse.setErrCode(MISSING_KEY_ERROR);
                        } else {
                            kvResponse.setErrCode(SUCCESS);
                        }
                    }
                }
                // Shutdown
                case 4 -> System.exit(0);
                // Clear
                case 5 -> {
                    this.store.clear();
                    kvResponse.setErrCode(SUCCESS);
                    Runtime.getRuntime().gc();
                }
                // Health
                case 6 -> kvResponse.setErrCode(SUCCESS);
                // PID
                case 7 -> {
                    kvResponse.setErrCode(SUCCESS);
                    kvResponse.setPid((int) ProcessHandle.current().pid());
                }
                // Membership Count
                case 8 -> {
                    kvResponse.setErrCode(SUCCESS);
                    kvResponse.setMembershipCount(1);
                }
                // Unknown
                default -> kvResponse.setErrCode(UNRECOGNIZED_ERROR);
            }
        } catch (IOException e) {
            System.out.println("Overload Error: " + Utils.getFreeMemory());
            kvResponse.setErrCode(OVERLOAD_ERROR);
            kvResponse.setOverloadWaitTime(this.waitTime);
        } catch (OutOfMemoryError e) {
            System.out.println("Memory Error: " + Utils.getFreeMemory());
            kvResponse.setErrCode(MEMORY_ERROR);
            Runtime.getRuntime().gc();
        } catch (Exception e) {
            kvResponse.setErrCode(STORE_ERROR);
        } finally {
            try {
                resMsg.setPayload(kvResponse.build().toByteString());
                resMsg.setCheckSum(Utils.createCheckSum(resMsg.getMessageID().toByteArray(), resMsg.getPayload().toByteArray()));
                byte[] response = resMsg.build().toByteArray();
                DatagramPacket resPacket = new DatagramPacket(response, response.length, this.packet.getAddress(), this.packet.getPort());
                this.socket.send(resPacket);
                this.cache.put(resMsg.getMessageID(), resMsg.getPayload().toByteArray());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
