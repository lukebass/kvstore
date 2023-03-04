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

    private final Cache<Key, byte[]> cache;

    private final int waitTime;

    public ServerResponse(DatagramSocket socket, DatagramPacket packet, Store store, Cache<Key, byte[]> cache, int waitTime) {
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

            byte[] cacheValue = this.cache.getIfPresent(new Key(reqMsg.getMessageID().toByteArray()));
            if (Utils.isCheckSumInvalid(reqMsg.getCheckSum(), reqMsg.getMessageID().toByteArray(), reqMsg.getPayload().toByteArray())) {
                return;
            } else if (cacheValue != null) {
                DatagramPacket resPacket = new DatagramPacket(cacheValue, cacheValue.length, this.packet.getAddress(), this.packet.getPort());
                this.socket.send(resPacket);
                return;
            } else if (Utils.isOutOfMemory()) {
                if (cache.size() > Utils.MAX_CACHE_SIZE) throw new IOException("Too many requests");
                throw new OutOfMemoryError("Out of memory");
            }

            switch (kvRequest.getCommand()) {
                case Utils.PUT_REQUEST -> {
                    if (kvRequest.getKey().size() == 0 || kvRequest.getKey().size() > 32) {
                        kvResponse.setErrCode(Utils.INVALID_KEY_ERROR);
                    } else if (kvRequest.getValue().size() == 0 || kvRequest.getValue().size() > 10000) {
                        kvResponse.setErrCode(Utils.INVALID_VALUE_ERROR);
                    } else {
                        this.store.put(kvRequest.getKey().toByteArray(), kvRequest.getValue().toByteArray(), kvRequest.getVersion());
                        kvResponse.setErrCode(Utils.SUCCESS);
                    }
                }
                case Utils.GET_REQUEST -> {
                    if (kvRequest.getKey().size() == 0 || kvRequest.getKey().size() > 32) {
                        kvResponse.setErrCode(Utils.INVALID_KEY_ERROR);
                    } else {
                        byte[] composite = this.store.get(kvRequest.getKey().toByteArray());
                        if (composite == null) {
                            kvResponse.setErrCode(Utils.MISSING_KEY_ERROR);
                        } else {
                            buffer = ByteBuffer.wrap(composite);
                            int version = buffer.getInt();
                            byte[] value = new byte[composite.length - 4];
                            buffer.get(value);
                            kvResponse.setErrCode(Utils.SUCCESS);
                            kvResponse.setValue(ByteString.copyFrom(value));
                            kvResponse.setVersion(version);
                        }
                    }
                }
                case Utils.REMOVE_REQUEST -> {
                    if (kvRequest.getKey().size() == 0 || kvRequest.getKey().size() > 32) {
                        kvResponse.setErrCode(Utils.INVALID_KEY_ERROR);
                    } else {
                        byte[] composite = this.store.remove(kvRequest.getKey().toByteArray());
                        if (composite == null) {
                            kvResponse.setErrCode(Utils.MISSING_KEY_ERROR);
                        } else {
                            kvResponse.setErrCode(Utils.SUCCESS);
                        }
                    }
                }
                case Utils.SHUTDOWN_REQUEST -> System.exit(0);
                case Utils.CLEAR_REQUEST -> {
                    kvResponse.setErrCode(Utils.SUCCESS);
                    this.store.clear();
                    this.cache.invalidateAll();
                    System.gc();
                }
                case Utils.HEALTH_REQUEST -> kvResponse.setErrCode(Utils.SUCCESS);
                case Utils.PID_REQUEST -> {
                    kvResponse.setErrCode(Utils.SUCCESS);
                    kvResponse.setPid((int) ProcessHandle.current().pid());
                }
                case Utils.MEMBERSHIP_REQUEST -> {
                    kvResponse.setErrCode(Utils.SUCCESS);
                    kvResponse.setMembershipCount(1);
                }
                default -> kvResponse.setErrCode(Utils.UNRECOGNIZED_ERROR);
            }
        } catch (IOException e) {
            System.out.println("Overload Error: " + Utils.getFreeMemory());
            kvResponse.setErrCode(Utils.OVERLOAD_ERROR);
            kvResponse.setOverloadWaitTime(this.waitTime);
            System.gc();
        } catch (OutOfMemoryError e) {
            System.out.println("Memory Error: " + Utils.getFreeMemory());
            kvResponse.setErrCode(Utils.MEMORY_ERROR);
            System.gc();
        } catch (Exception e) {
            kvResponse.setErrCode(Utils.STORE_ERROR);
            System.gc();
        } finally {
            try {
                resMsg.setPayload(kvResponse.build().toByteString());
                resMsg.setCheckSum(Utils.createCheckSum(resMsg.getMessageID().toByteArray(), resMsg.getPayload().toByteArray()));
                byte[] response = resMsg.build().toByteArray();
                DatagramPacket resPacket = new DatagramPacket(response, response.length, this.packet.getAddress(), this.packet.getPort());
                this.socket.send(resPacket);
                this.cache.put(new Key(resMsg.getMessageID().toByteArray()), response);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
