package com.solace.aaron.conflate;

public interface PayloadUtils {

    
    public byte[] merge(byte[] orig, byte[] update);
    public byte[] getPayload();
    
    
    
}
