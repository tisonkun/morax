/*
 * Copyright 2023 tison <wander4096@gmail.com>
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

package org.tisonkun.morax.proto.exception;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Utility to make it easier to add context to exception messages.
 */
public class ExceptionMessageBuilder {
    public static ExceptionMessageBuilder exMsg(String message) {
        return new ExceptionMessageBuilder(message);
    }

    private final List<String> kvs = new ArrayList<>();
    private final String message;

    ExceptionMessageBuilder(String message) {
        this.message = message;
    }

    public ExceptionMessageBuilder kv(String key, Object value) {
        kvs.add(key + "=" + value);
        return this;
    }

    @Override
    public String toString() {
        final String prefix = message + " (";
        final String suffix = ")";
        return kvs.stream().collect(Collectors.joining(",", prefix, suffix));
    }
}
