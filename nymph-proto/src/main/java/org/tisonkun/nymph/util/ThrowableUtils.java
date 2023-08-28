package org.tisonkun.nymph.util;

import java.util.concurrent.CompletionException;
import lombok.experimental.UtilityClass;

@UtilityClass
public class ThrowableUtils {
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
     * @param typeToStrip type to strip
     * @return Unpacked cause or given Throwable if not packed
     */
    public static Throwable stripException(
            Throwable throwableToStrip,
            Class<? extends Throwable> typeToStrip) {
        while (typeToStrip.isAssignableFrom(throwableToStrip.getClass())
                && throwableToStrip.getCause() != null) {
            throwableToStrip = throwableToStrip.getCause();
        }
        return throwableToStrip;
    }
}
