package com.solace.aaron.conflate;

import java.nio.charset.Charset;

public class JsonPatchTests {

    static final Charset utf_8 = Charset.forName("UTF-8");
    
    public static void main(String... args) {
        
        JsonTopicMap map = new JsonTopicMap();
        
        
        String orig = "{ }";
        String patch = "[ {\r\n" + 
                "  \"op\" : \"add\",\r\n" + 
                "  \"path\" : \"/a\",\r\n" + 
                "  \"value\" : \"b\"\r\n" + 
                "} ]";
        
        map.insert("a/b",JsonTopicMap.convert(orig.getBytes(utf_8)));
        
        map.merge("a/b",JsonTopicMap.convert(patch.getBytes(utf_8)));
        
        System.out.println(map.getPayloadValue("a/b").toString());
        
        
    }
    
    
    
    
    
    
    
    
    
}
