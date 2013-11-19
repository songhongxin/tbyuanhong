package com.etao.lz.star.output;

import java.util.Map;

import com.etao.lz.star.StarConfig;
import com.google.protobuf.GeneratedMessage;

public interface LogOutput extends java.io.Serializable{
    void config(StarConfig config);
    void open(int id);
    void output(byte[] data, GeneratedMessage log);
    
    void getStat(Map<String, String> map);
}
