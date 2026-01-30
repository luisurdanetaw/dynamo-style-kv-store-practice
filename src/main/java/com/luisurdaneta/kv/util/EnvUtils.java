package com.luisurdaneta.kv.util;

public final class EnvUtils {
    private EnvUtils() {}

    public static String env(String k, String def) {
        String v = System.getenv(k);
        return (v == null || v.isBlank()) ? def : v.trim();
    }

    public static int intEnv(String k, int def) {
        String v = System.getenv(k);
        if (v == null || v.isBlank()) return def;
        return Integer.parseInt(v.trim());
    }
}
