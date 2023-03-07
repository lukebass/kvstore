package com.s62023080.CPEN431.A4;

import ca.NetSysLab.ProtocolBuffers.Message.Msg;
import ca.NetSysLab.ProtocolBuffers.KeyValueRequest.KVRequest;
import ca.NetSysLab.ProtocolBuffers.KeyValueResponse.KVResponse;
import com.google.common.cache.Cache;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.net.*;
import java.util.concurrent.ConcurrentSkipListMap;

public class ServerResponse implements Runnable {
    private final DatagramSocket socket;

    private final DatagramPacket packet;

    private InetAddress address;

    private int port;

    private final Store store;

    private final Cache<ByteString, byte[]> cache;

    private final ConcurrentSkipListMap<Integer, Integer> addresses;

    private final ConcurrentSkipListMap<Integer, int[]> tables;

    public ServerResponse(DatagramSocket socket, DatagramPacket packet, Store store, Cache<ByteString, byte[]> cache, ConcurrentSkipListMap<Integer, Integer> addresses, ConcurrentSkipListMap<Integer, int[]> tables) {
        this.socket = socket;
        this.packet = packet;
        this.address = packet.getAddress();
        this.port = packet.getPort();
        this.store = store;
        this.cache = cache;
        this.addresses = addresses;
        this.tables = tables;
    }

    public boolean isKeyInvalid(ByteString key) {
        return key.size() == 0 || key.size() > 32;
    }

    public boolean isValueInvalid(ByteString value) {
        return value.size() == 0 || value.size() > 10000;
    }

    public void setReturnLocation(Msg msg) throws UnknownHostException {
        if (msg.getAddress().size() != 0) this.address = InetAddress.getByAddress(msg.getAddress().toByteArray());
        if (msg.getPort() != 0) this.port = msg.getPort();
    }

    public void redirectRequest(Msg msg, int nodeID) {
        try {
            Msg.Builder clone = Msg.newBuilder();
            clone.setMessageID(msg.getMessageID());
            clone.setPayload(msg.getPayload());
            clone.setCheckSum(msg.getCheckSum());
            clone.setAddress(ByteString.copyFrom(this.address.getAddress()));
            clone.setPort(this.port);
            byte[] response = clone.build().toByteArray();
            this.socket.send(new DatagramPacket(response, response.length, InetAddress.getLocalHost(), this.addresses.get(nodeID)));
            System.out.println("Redirect to " + this.addresses.get(nodeID));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void run() {
        Msg.Builder resMsg = Msg.newBuilder();
        KVResponse.Builder kvResponse = KVResponse.newBuilder();

        try {
            ByteBuffer buffer = ByteBuffer.wrap(this.packet.getData());
            byte[] request = new byte[this.packet.getLength()];
            buffer.get(request);
            Msg reqMsg = Msg.parseFrom(request);
            if (Utils.isCheckSumInvalid(reqMsg.getCheckSum(), reqMsg.getMessageID().toByteArray(), reqMsg.getPayload().toByteArray())) return;

            setReturnLocation(reqMsg);
            resMsg.setMessageID(reqMsg.getMessageID());
            KVRequest kvRequest = KVRequest.parseFrom(reqMsg.getPayload());

            byte[] cacheValue = this.cache.getIfPresent(reqMsg.getMessageID());
            if (Utils.isCheckSumInvalid(reqMsg.getCheckSum(), reqMsg.getMessageID().toByteArray(), reqMsg.getPayload().toByteArray())) {
                return;
            } else if (cacheValue != null) {
                this.socket.send(new DatagramPacket(cacheValue, cacheValue.length, this.address, this.port));
                return;
            } else if (this.cache.size() > Utils.MAX_CACHE_SIZE && Utils.isOutOfMemory(Utils.UPPER_MIN_MEMORY)) {
                throw new IOException("Too many requests");
            } else if (Utils.isOutOfMemory(Utils.LOWER_MIN_MEMORY)) {
                throw new OutOfMemoryError("Out of memory");
            }

            switch (kvRequest.getCommand()) {
                case Utils.PUT_REQUEST -> {
                    if (isKeyInvalid(kvRequest.getKey())) {
                        kvResponse.setErrCode(Utils.INVALID_KEY_ERROR);
                    } else if (isValueInvalid(kvRequest.getValue())) {
                        kvResponse.setErrCode(Utils.INVALID_VALUE_ERROR);
                    } else {
                        if (!Utils.isLocalKey(kvRequest.getKey().toByteArray(), this.tables)) {
                            redirectRequest(reqMsg, Utils.searchTables(kvRequest.getKey().toByteArray(), this.tables));
                            return;
                        }

                        this.store.put(kvRequest.getKey(), kvRequest.getValue(), kvRequest.getVersion());
                        kvResponse.setErrCode(Utils.SUCCESS);
                        System.out.println("Put key: " + kvRequest.getKey());
                        System.out.println("Put value: " + kvRequest.getValue());
                        System.out.println("Put version: " + kvRequest.getVersion());
                    }
                }
                case Utils.GET_REQUEST -> {
                    if (isKeyInvalid(kvRequest.getKey())) {
                        kvResponse.setErrCode(Utils.INVALID_KEY_ERROR);
                    } else {
                        if (!Utils.isLocalKey(kvRequest.getKey().toByteArray(), this.tables)) {
                            redirectRequest(reqMsg, Utils.searchTables(kvRequest.getKey().toByteArray(), this.tables));
                            return;
                        }

                        Data data = this.store.get(kvRequest.getKey());
                        if (data == null) {
                            kvResponse.setErrCode(Utils.MISSING_KEY_ERROR);
                        } else {
                            kvResponse.setErrCode(Utils.SUCCESS);
                            kvResponse.setValue(data.value);
                            kvResponse.setVersion(data.version);
                            System.out.println("Get key: " + kvRequest.getKey());
                            System.out.println("Get value: " + data.value);
                            System.out.println("Get version: " + data.version);
                        }
                    }
                }
                case Utils.REMOVE_REQUEST -> {
                    if (isKeyInvalid(kvRequest.getKey())) {
                        kvResponse.setErrCode(Utils.INVALID_KEY_ERROR);
                    } else {
                        if (!Utils.isLocalKey(kvRequest.getKey().toByteArray(), this.tables)) {
                            redirectRequest(reqMsg, Utils.searchTables(kvRequest.getKey().toByteArray(), this.tables));
                            return;
                        }

                        Data data = this.store.remove(kvRequest.getKey());
                        if (data == null) {
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
            kvResponse.setOverloadWaitTime(Utils.OVERLOAD_TIME);
        } catch (OutOfMemoryError e) {
            System.out.println("Memory Error: " + Utils.getFreeMemory());
            kvResponse.setErrCode(Utils.MEMORY_ERROR);
        } catch (Exception e) {
            kvResponse.setErrCode(Utils.STORE_ERROR);
        }

        try {
            resMsg.setPayload(kvResponse.build().toByteString());
            resMsg.setCheckSum(Utils.createCheckSum(resMsg.getMessageID().toByteArray(), resMsg.getPayload().toByteArray()));
            byte[] response = resMsg.build().toByteArray();
            this.socket.send(new DatagramPacket(response, response.length, this.address, this.port));
            this.cache.put(resMsg.getMessageID(), response);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
