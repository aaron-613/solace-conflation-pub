package com.solace.aaron.conflate;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonPatch;
import javax.json.JsonStructure;
import javax.json.JsonValue;



/**
 * Discussion:
 * <ul>
 * <li>How do we deal with multiple conflation windows of different lengths?</li>
 * </ul>
 * @author Aaron Lee
 *
 */
public class JsonTopicCache {

    
    Map<String,JsonStructure> topicMap = new HashMap<>();
    Map<String,JsonStructure> previous = new HashMap<>();
    
    
    Map<String,JsonStructure> jsonMap = new HashMap<>();
    Map<String,List<String>> rawMap = new HashMap<>();
    
    
    /**
     * Just stick it in!
     * 
     * @param topic
     * @param payload
     */
    void post(String topic, JsonStructure payload) {
        topicMap.put(topic,payload);
    }
    
    // two copies
    void post(String topic, String payload) {
        List<String> cleanList = new ArrayList<>(); 
        cleanList.add(payload);
        rawMap.put(topic,cleanList);
        jsonMap.put(topic,JsonPatchUtils.strToJson(payload));
    }
    
    JsonStructure patch(String topic, String payload) {
        JsonArray jsonA = null;
        try {
            jsonA = JsonPatchUtils.strToJson(payload).asJsonArray();
            // if we get here, then it's a valid array
            rawMap.get(topic).add(payload);  // add to the back of the list
            JsonPatch jPatch= Json.createPatch(jsonA);  // create the patching object
            JsonStructure currentJson;
            currentJson = jsonMap.get(topic);
            if (currentJson == null) {  // nothing in there
                currentJson = previous.get(topic);
                if (currentJson == null) {  // trying to patch an uninitialized value!
                    // throw something?
                    throw new NullPointerException("You are trying to PATCH on topic "+topic+" but no previous value exists!");
                } else {
                    JsonStructure s = jPatch.apply(get(topic));
                    jsonMap.put(topic,s);
                    return s;
                }
            } else {
                JsonStructure s = jPatch.apply(get(topic));
                jsonMap.put(topic,s);
                return s;
            }
        } catch (ClassCastException e) {
            throw new IllegalArgumentException("The PATCH payload submitted was not a JSON array! "+payload);
        }
    }
    
    
    /* 
     * Called when the conflation/eliding timer window expires
     * 
     *  - NOTE: cache request comes in, provide the PREVIOUS (not current) value, so conflated update makes sense + fairness
     * 
     *  - Copy "previous" into "old previous"
     *  - Copy "current" into previous"
     *  - Initialize "current" with the current objects?  If a cache request comes, do we do the eliding then?
     *  - 
     *  - For each element:
     *     - build the patch object by taking all the updates together
     *     - calculate the delta/diff between the current, and the previous (which could be 0 even if it has updates)
     *     - store the new (merged) value as the "previous"
     *  - Need to copy the 'current' array into a 'previous' array 
     *  - 
     *  
     *  
     *  
     *  
     */
    void tick() {
        synchronized (this) {
            previous = topicMap;
            topicMap = new HashMap<>();
//            for (Entry<String, JsonStructure> entry : previous.entrySet()) {
//                topicMap.put(entry.getKey(),entry.getValue().)
//            }
        }
        // now topicMap is empty, and can continue using it
        synchronized (previous) {
            // here we're going to calculate the deltas and whatnot
            
        }
    }
    
    
    
    

    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    /**
     * Need to look at the previous version, and do a delta
     * 
     * @param topic
     * @param patch
     * @return
     */
    JsonStructure patch(String topic, JsonValue patch) {
        JsonPatch jPatch= Json.createPatch((JsonArray) patch);  // create the patching object
        JsonStructure currentJson;
        currentJson = get(topic);
        if (currentJson == null) {  // nothing in there
            currentJson = previous.get(topic);
            if (currentJson == null) {  // trying to patch an uninitialized value!
                // throw something?
                throw new NullPointerException("You are trying to PATCH on topic "+topic+" but no previous value exists!");
            } else {
                JsonStructure s = jPatch.apply(get(topic));
                post(topic,s);
                return s;
            }
        } else {
            JsonStructure s = jPatch.apply(get(topic));
            post(topic,s);
            return s;
        }
//        JsonMergePatch mergePatch = Json.createMergePatch(patch);
//        JsonValue merged = mergePatch.apply(getPayloadValue(topic));
//        insert(topic,merged);
    }
    
    JsonStructure get(String topic) {
        return topicMap.get(topic);
    }
    
    JsonStructure delete(String topic) {
        return topicMap.remove(topic);
    }
    
    
    
}
