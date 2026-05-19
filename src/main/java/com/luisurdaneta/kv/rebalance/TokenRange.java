package com.luisurdaneta.kv.rebalance;

/**
 * A half-open arc (start, end] in the consistent hash ring's 64-bit token space.
 * When start > end the arc wraps around Long.MAX_VALUE → Long.MIN_VALUE.
 */
public record TokenRange(long start, long end) {

    public boolean contains(long hash) {
        if (start < end) return hash > start && hash <= end;
        // wrap-around: covers [start+1 … MAX_VALUE] ∪ [MIN_VALUE … end]
        return hash > start || hash <= end;
    }
}
