package com.luisurdaneta.kv.core.ports;

public interface Clock {
    long nowMillis();

    static Clock system() {
        return System::currentTimeMillis;
    }
}
