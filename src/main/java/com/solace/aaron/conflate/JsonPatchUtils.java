package com.solace.aaron.conflate;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.charset.Charset;

import javax.json.Json;
import javax.json.JsonMergePatch;
import javax.json.JsonReader;
import javax.json.JsonReaderFactory;
import javax.json.JsonStructure;
import javax.json.JsonValue;
import javax.json.JsonWriter;
import javax.json.JsonWriterFactory;

public class JsonPatchUtils {

    private static final JsonWriterFactory WRITER_FACTORY = Json.createWriterFactory(null);
    private static final JsonReaderFactory READER_FACTORY = Json.createReaderFactory(null);
    private static final Charset UTF_8 = Charset.forName("UTF-8");


    static JsonStructure bytesToJson(byte[] ba) {
        JsonReader reader = READER_FACTORY.createReader(new ByteArrayInputStream(ba),UTF_8);
        JsonStructure json = reader.read();
        return json;
    }
    
    static JsonStructure strToJson(String str) {
        JsonReader reader = READER_FACTORY.createReader(new StringReader(str));
        JsonStructure json = reader.read();
        return json;
    }
    
    static JsonValue diff(JsonStructure source, JsonStructure target) {

        JsonMergePatch mergePatch = Json.createMergeDiff(source,target);
        return mergePatch.toJsonValue();
        
        
        
        
        
        
    }
    

    static byte[] jsonToBytes(JsonStructure json) {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        byte[] returnArray;
        JsonWriter writer = WRITER_FACTORY.createWriter(buffer,UTF_8);
        writer.write(json);
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

    static String jsonToStr(JsonStructure json) {
        StringWriter strWriter = new StringWriter();
        WRITER_FACTORY.createWriter(strWriter);
        JsonWriter writer = WRITER_FACTORY.createWriter(strWriter);
        writer.write(json);
        return strWriter.toString();
    }
    
    
    
}
