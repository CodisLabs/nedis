/**
 * Copyright (c) 2015 CodisLabs.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.codis.nedis.protocol;

/**
 * @author Apache9
 * @see http://redis.io/commands/set
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
