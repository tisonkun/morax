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

package org.tisonkun.morax.proto.util;

import java.util.concurrent.CompletionException;
import lombok.experimental.UtilityClass;

@UtilityClass
public class ThrowableUtils {
    public static RuntimeException sneakyThrow(Throwable t) {
        if (t == null) throw new NullPointerException("t");
        return sneakyThrow0(t);
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> T sneakyThrow0(Throwable t) throws T {
        throw (T) t;
    }

    /**
     * Unpacks an {@link CompletionException} and returns its cause. Otherwise, the given Throwable
     * is returned.
     *
     * @param throwable to unpack if it is an CompletionException
     * @return Cause of CompletionException or given Throwable
     */
    public static Throwable stripCompletionException(Throwable throwable) {
        return stripException(throwable, CompletionException.class);
    }

    /**
     * Unpacks a specified exception and returns its cause. Otherwise, the given {@link Throwable}
     * is returned.
     *
     * @param throwableToStrip to strip
     * @param typeToStrip      type to strip
     * @return Unpacked cause or given Throwable if not packed
     */
    public static Throwable stripException(Throwable throwableToStrip, Class<? extends Throwable> typeToStrip) {
        while (typeToStrip.isAssignableFrom(throwableToStrip.getClass()) && throwableToStrip.getCause() != null) {
            throwableToStrip = throwableToStrip.getCause();
        }
        return throwableToStrip;
    }
}
