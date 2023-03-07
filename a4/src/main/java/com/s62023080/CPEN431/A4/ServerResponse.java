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

    private final Store store;

    private final Cache<ByteString, byte[]> cache;

    private final ConcurrentSkipListMap<Integer, Integer> addresses;

    private final ConcurrentSkipListMap<Integer, int[]> tables;

    public ServerResponse(DatagramSocket socket, DatagramPacket packet, Store store, Cache<ByteString, byte[]> cache, ConcurrentSkipListMap<Integer, Integer> addresses, ConcurrentSkipListMap<Integer, int[]> tables) {
        this.socket = socket;
        this.packet = packet;
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

    public void redirectRequest(Msg reqMsg, int nodeID) {
        try {
            Msg.Builder resMsg = Msg.newBuilder();
            resMsg.setMessageID(reqMsg.getMessageID());
            resMsg.setPayload(reqMsg.getPayload());
            resMsg.setCheckSum(reqMsg.getCheckSum());
            resMsg.setAddress(reqMsg.getAddress().size() > 0 ? reqMsg.getAddress() : ByteString.copyFrom(this.packet.getAddress().getAddress()));
            resMsg.setPort(reqMsg.getPort() != 0 ? reqMsg.getPort() : this.packet.getPort());
            byte[] response = resMsg.build().toByteArray();
            this.socket.send(new DatagramPacket(response, response.length, InetAddress.getLocalHost(), this.addresses.get(nodeID)));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void run() {
        Msg.Builder resMsg = Msg.newBuilder();
        KVResponse.Builder kvResponse = KVResponse.newBuilder();
        Msg reqMsg = null;

        try {
            ByteBuffer buffer = ByteBuffer.wrap(this.packet.getData());
            byte[] request = new byte[this.packet.getLength()];
            buffer.get(request);
            reqMsg = Msg.parseFrom(request);
            resMsg.setMessageID(reqMsg.getMessageID());
            KVRequest kvRequest = KVRequest.parseFrom(reqMsg.getPayload());

            byte[] cacheValue = this.cache.getIfPresent(reqMsg.getMessageID());
            if (Utils.isCheckSumInvalid(reqMsg.getCheckSum(), reqMsg.getMessageID().toByteArray(), reqMsg.getPayload().toByteArray())) {
                return;
            } else if (cacheValue != null) {
                this.socket.send(new DatagramPacket(cacheValue, cacheValue.length, this.packet.getAddress(), this.packet.getPort()));
                return;
            } else if (Utils.isOutOfMemory()) {
                if (this.cache.size() > Utils.MAX_CACHE_SIZE) throw new IOException("Too many requests");
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
            System.out.println("Overload Error: " + Utils.getUsedMemory());
            kvResponse.setErrCode(Utils.OVERLOAD_ERROR);
            kvResponse.setOverloadWaitTime(Utils.OVERLOAD_TIME);
        } catch (OutOfMemoryError e) {
            System.out.println("Memory Error: " + Utils.getUsedMemory());
            kvResponse.setErrCode(Utils.MEMORY_ERROR);
        } catch (Exception e) {
            kvResponse.setErrCode(Utils.STORE_ERROR);
        } finally {
            try {
                resMsg.setPayload(kvResponse.build().toByteString());
                resMsg.setCheckSum(Utils.createCheckSum(resMsg.getMessageID().toByteArray(), resMsg.getPayload().toByteArray()));
                byte[] response = resMsg.build().toByteArray();
                this.socket.send(new DatagramPacket(
                        response,
                        response.length,
                        reqMsg.getAddress().size() > 0 ? InetAddress.getByAddress(reqMsg.getAddress().toByteArray()) : this.packet.getAddress(),
                        reqMsg.getPort() != 0 ? reqMsg.getPort() : this.packet.getPort()
                ));
                this.cache.put(reqMsg.getMessageID(), response);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
