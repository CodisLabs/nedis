package com.wandoulabs.nedis.protocol;

/**
 * @author Apache9
 */
public class SetParams {

    private long ex;

    private long px;

    private boolean nx;

    private boolean xx;

    public SetParams setEx(long seconds) {
        this.ex = seconds;
        this.px = 0L;
        return this;
    }

    public SetParams setPx(long millis) {
        this.px = millis;
        this.ex = 0L;
        return this;
    }

    public SetParams setNx() {
        this.nx = true;
        this.xx = false;
        return this;
    }

    public SetParams clearNx() {
        this.nx = false;
        return this;
    }

    public SetParams setXx() {
        this.xx = true;
        this.nx = false;
        return this;
    }

    public SetParams clearXx() {
        this.xx = false;
        return this;
    }

    public long ex() {
        return ex;
    }

    public long px() {
        return px;
    }

    public boolean nx() {
        return nx;
    }

    public boolean xx() {
        return xx;
    }
}
