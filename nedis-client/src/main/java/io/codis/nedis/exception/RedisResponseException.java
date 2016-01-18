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
package io.codis.nedis.exception;

import java.io.IOException;

/**
 * @author Apache9
 */
public class RedisResponseException extends IOException {

    private static final long serialVersionUID = 1208471190036181159L;

    public RedisResponseException() {
        super();
    }

    public RedisResponseException(String message, Throwable cause) {
        super(message, cause);
    }

    public RedisResponseException(String message) {
        super(message);
    }

    public RedisResponseException(Throwable cause) {
        super(cause);
    }

}
