package ru.turbogoose.cca.backend.common.util;

public class LongCounter {
    private long counter;

    public LongCounter(long init) {
        this.counter = init;
    }

    public void increment() {
        counter++;
    }

    public long get() {
        return counter;
    }
}
