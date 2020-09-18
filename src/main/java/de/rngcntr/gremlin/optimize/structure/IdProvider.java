package de.rngcntr.gremlin.optimize.structure;

public class IdProvider {
    private static int nextId = 0;
    private static final IdProvider INSTANCE = new IdProvider();

    public static IdProvider getInstance() {
        return INSTANCE;
    }

    public synchronized int getNextId() {
        return nextId++;
    }
}
