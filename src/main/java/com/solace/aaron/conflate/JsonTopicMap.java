package com.solace.aaron.conflate;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import javax.json.Json;
import javax.json.JsonMergePatch;
import javax.json.JsonReader;
import javax.json.JsonValue;
import javax.json.JsonWriter;

public class JsonTopicMap {

    private static final Charset UTF_8 = Charset.forName("UTF-8");

    Map<String,JsonValue> topicMap = new HashMap<>();  // a 
    
    
    static JsonValue convert(byte[] ba) {
        //JsonReader reader = Json.createReader(new StringReader(new String(ba,UTF_8)));
        JsonReader reader = Json.createReader(new ByteArrayInputStream(ba));
        JsonValue jv = reader.readValue();
        return jv;
    }
    
    
    
    void insert(String topic, JsonValue payload) {
        topicMap.put(topic,payload);
    }
    
    void merge(String topic, JsonValue patch) {
        JsonMergePatch mergePatch = Json.createMergePatch(patch);
        JsonValue merged = mergePatch.apply(getPayloadValue(topic));
        insert(topic,merged);
    }
    
    JsonValue getPayloadValue(String topic) {
        return topicMap.get(topic);
    }
    
    byte[] getPayloadBytes(String topic) {
        JsonValue js = getPayloadValue(topic);
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        byte[] returnArray;
        JsonWriter writer = Json.createWriter(buffer);
        writer.write(js);
        try {
            buffer.flush();
            returnArray = buffer.toByteArray();
            buffer.close();
        } catch (IOException e) {
            e.printStackTrace();
            returnArray = new byte[0];
        }
        return returnArray;
    }
    
    
}
