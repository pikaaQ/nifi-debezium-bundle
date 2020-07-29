package org.apache.nifi.debezium.processors;

import org.apache.kafka.common.utils.ThreadUtils;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.storage.MemoryOffsetBackingStore;
import org.apache.kafka.connect.storage.OffsetBackingStore;
import org.apache.kafka.connect.util.Callback;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.*;

public class OffsetPointer implements OffsetBackingStore {

    private static final Logger log = LoggerFactory.getLogger(OffsetPointer.class);
    protected static ConcurrentHashMap<String, Map<String, String>> offsetMap = new ConcurrentHashMap<>();
    protected ExecutorService executor;
    protected String topic;


    @Override
    public void start() {
        this.executor = Executors.newFixedThreadPool(1, ThreadUtils.createThreadFactory(this.getClass().getSimpleName() + "-%d", false));
    }

    @Override
    public void stop() {
        if (this.executor != null) {
            this.executor.shutdown();

            try {
                this.executor.awaitTermination(5L, TimeUnit.SECONDS);
            } catch (InterruptedException var2) {
                Thread.currentThread().interrupt();
            }

            if (!this.executor.shutdownNow().isEmpty()) {
                throw new ConnectException("Failed to stop MemoryOffsetBackingStore. Exiting without cleanly shutting down pending tasks and/or callbacks.");
            }

            this.executor = null;
        }
    }

    @Override
    public Future<Map<ByteBuffer, ByteBuffer>> get(Collection<ByteBuffer> keys) {
        return this.executor.submit(new Callable<Map<ByteBuffer, ByteBuffer>>() {
            public Map<ByteBuffer, ByteBuffer> call() throws Exception {
                Map<ByteBuffer, ByteBuffer> result = new HashMap();
                if(offsetMap.containsKey(topic)){
                    Map<String, String> stateMap = offsetMap.get(topic);
                    Iterator itor = keys.iterator();

                    while(itor.hasNext()) {
                        ByteBuffer key = (ByteBuffer)itor.next();
                        result.put(key, getBuffer(stateMap.get(key)));
                    }
                }

                return result;
            }
        });
    }

    @Override
    public Future<Void> set(Map<ByteBuffer, ByteBuffer> values, Callback<Void> callback) {
        return this.executor.submit(new Callable<Void>() {
            public Void call() throws Exception {
                Iterator itor = values.entrySet().iterator();
                Map<String, String> newStateMap = new HashMap<>();
                while (itor.hasNext()) {
                    Map.Entry<ByteBuffer, ByteBuffer> entry = (Map.Entry) itor.next();
                    newStateMap.put(getString(entry.getKey()), getString(entry.getValue()));
                }
                offsetMap.put(topic, newStateMap);

                if (callback != null) {
                    callback.onCompletion((Throwable) null, null);
                }
                return null;
            }
        });
    }

    @Override
    public void configure(WorkerConfig workerConfig) {
        this.topic = workerConfig.getString("offset.storage.topic");
    }

    public static Map<String, String> getTopicPointer(String topic){
        HashMap<String, String> result = new HashMap<>();
        if(offsetMap.containsKey(topic)){
            result.putAll(offsetMap.get(topic));
        }
        return result;
    }

    public static void setTopicPointer(String topic, Map<String, String> map){
        offsetMap.put(topic, map);
    }

    private static ByteBuffer getBuffer(String s){
        return s == null ? null : ByteBuffer.wrap(s.getBytes());
    }

    private static String getString(ByteBuffer buffer) {

        if(buffer == null){
            return null;
        }
        Charset charset = null;
        CharsetDecoder decoder = null;
        CharBuffer charBuffer = null;

        try {
            charset = Charset.forName("UTF-8");
            decoder = charset.newDecoder();
            //用这个的话，只能输出来一次结果，第二次显示为空
            // charBuffer = decoder.decode(buffer);

            charBuffer = decoder.decode(buffer.asReadOnlyBuffer());
            return charBuffer.toString();
        } catch (Exception ex) {
            log.error("get data from store error:", ex);
            return null;
        }

    }
}
