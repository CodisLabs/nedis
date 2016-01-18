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
package io.codis.nedis.handler;

import io.netty.buffer.ByteBuf;
import io.netty.util.concurrent.Promise;

/**
 * @author Apache9
 */
public class RedisRequest {

    private final Promise<Object> promise;

    private final ByteBuf params;

    public RedisRequest(Promise<Object> promise, ByteBuf params) {
        this.promise = promise;
        this.params = params;
    }

    public Promise<Object> getPromise() {
        return promise;
    }

    public ByteBuf getParams() {
        return params;
    }
}
