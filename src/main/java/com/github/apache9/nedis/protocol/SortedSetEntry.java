package com.github.apache9.nedis.protocol;

/**
 * @author Apache9
 */
public class SortedSetEntry {

    private final byte[] member;

    private final double score;

    public SortedSetEntry(byte[] member, double score) {
        this.member = member;
        this.score = score;
    }

    public byte[] member() {
        return member;
    }

    public double score() {
        return score;
    }

}
