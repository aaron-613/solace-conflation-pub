package com.solace.aaron.conflate;

import java.nio.charset.Charset;

import javax.json.JsonArray;
import javax.json.JsonStructure;
import javax.json.JsonValue;

public class JsonPatchTests {

    static final Charset utf_8 = Charset.forName("UTF-8");
    
    public static void main(String... args) {
        
        JsonTopicCache map = new JsonTopicCache();
        
        
        String orig = "{}";
        String patch = "[ { \"op\" : \"add\", \"path\" : \"/a\", \"value\" : \"b\" } ]";
//        orig = " [1, 2, 3, 4]";
//        patch = "[{\"op\": \"remove\", \"path\": \"/0\"}]";
        System.out.printf("Orig Str:  %s%n",orig);
        System.out.printf("Patch Str: %s%n%n",patch);
        
        JsonValue o = JsonPatchUtils.strToJson(orig);
        JsonValue p = JsonPatchUtils.strToJson(patch);
        System.out.printf("Orig Obj:  %s%n",o);
        System.out.printf("Patch Obj: %s%n%n",p);
        
        final String topic = "a/b";
        
        map.post(topic,JsonPatchUtils.strToJson(orig));
        System.out.printf("JsonValue in Map at %s: %s%n",topic,map.get(topic));
        System.out.println("Performing merge now...");
        map.patch(topic,JsonPatchUtils.strToJson(patch));
        System.out.printf("JsonValue in Map at %s: %s%n",topic,map.get(topic));
        JsonStructure v1 = map.get(topic);
        
        patch = "[ { \"op\" : \"add\", \"path\" : \"/b\", \"value\" : \"c\" } ]";
        map.patch(topic,JsonPatchUtils.strToJson(patch));
        System.out.printf("JsonValue in Map at %s: %s%n",topic,map.get(topic));
        
        System.out.printf("Diff: %s%n",JsonPatchUtils.diff(v1,map.get(topic)));

        
        
        JsonStructure j1 = JsonPatchUtils.strToJson(orig);
        try {
            JsonArray ja = j1.asJsonArray();
        } catch (ClassCastException e) {
            System.out.println("Hmmmmmmmm not an array!");
        }
        
        
        
        
    }
    
    
    
    
    
    
    
    
    
}
