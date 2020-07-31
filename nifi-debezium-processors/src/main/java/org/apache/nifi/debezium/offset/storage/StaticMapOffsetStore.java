package org.apache.nifi.debezium.offset.storage;

import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.storage.MemoryOffsetBackingStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class StaticMapOffsetStore extends MemoryOffsetBackingStore {

    private static final Logger log = LoggerFactory.getLogger(StaticMapOffsetStore.class);
    protected static ConcurrentHashMap<String, Map<String, String>> offsetMap = new ConcurrentHashMap<>();
    private String topic;
    private final static String CHARSET = "ISO-8859-1";

    public synchronized void start() {
        super.start();
        this.load();
    }

    private void load() {
        Map<String, String> topicOffset = offsetMap.get(topic);
        if(topicOffset != null){
            this.data = new HashMap();
            for(Map.Entry<String, String> mapEntry : topicOffset.entrySet()){
                try {
                    ByteBuffer key = mapEntry.getKey() != null ? ByteBuffer.wrap(mapEntry.getKey().getBytes(CHARSET)) : null;
                    ByteBuffer value = mapEntry.getValue() != null ? ByteBuffer.wrap(mapEntry.getValue().getBytes(CHARSET)) : null;
                    if(key != null){
                        this.data.put(key, value);
                    }
                } catch (UnsupportedEncodingException e) {
                    // should never happen, swallow the exception
                    log.error("load offset error.", e);
                }
            }
        }
    }

    protected void save() {
        Map<String, String> topicOffset = new HashMap();
        for(Map.Entry<ByteBuffer, ByteBuffer> mapEntry : this.data.entrySet()) {
            byte[] key = mapEntry.getKey() != null ? ((ByteBuffer)mapEntry.getKey()).array() : null;
            byte[] value = mapEntry.getValue() != null ? ((ByteBuffer)mapEntry.getValue()).array() : null;
            try {
                topicOffset.put(new String(key, CHARSET), new String(value, CHARSET));
            } catch (UnsupportedEncodingException e) {
                // should never happen, swallow the exception
                log.error("save offset error.", e);
            }
        }
        offsetMap.put(topic, topicOffset);
    }


    @Override
    public void configure(WorkerConfig workerConfig) {
        this.topic = workerConfig.getString("offset.storage.topic");
    }

    public static Map<String, String> getTopicOffset(String topic){
        HashMap<String, String> result = new HashMap<>();
        if(offsetMap.containsKey(topic)){
            result.putAll(offsetMap.get(topic));
        }
        return result;
    }

    public static void setTopicOffset(String topic, Map<String, String> map){
        HashMap<String, String> result = new HashMap<>();
        result.putAll(map);
        offsetMap.put(topic, result);
    }
}
