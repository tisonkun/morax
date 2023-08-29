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

package org.tisonkun.morax.util;

import java.util.concurrent.Callable;

/**
 * `DynamicVariables` provide a binding mechanism where the current
 * value is found through dynamic scope, but where access to the
 * variable itself is resolved through static scope.
 * <p>
 * The current value can be retrieved with the value method. New values
 * should be pushed using the `withValue` method. Values pushed via
 * `withValue` only stay valid while the `withValue`'s second argument, a
 * parameterless closure, executes. When the second argument finishes,
 * the variable reverts to the previous value.
 * <p>
 * Each thread gets its own stack of bindings.  When a
 * new thread is created, the `DynamicVariable` gets a copy
 * of the stack of bindings from the parent thread, and
 * from then on the bindings for the new thread
 * are independent of those for the original thread.
 */
// This class is ported from scala-library.
public class DynamicVariable<T> {
    private final InheritableThreadLocal<T> threadLocal;

    public DynamicVariable(T initialValue) {
        this.threadLocal = new InheritableThreadLocal<>() {
            @Override
            protected T initialValue() {
                return initialValue;
            }
        };
    }

    /**
     * Set the value of the variable while executing the specified thunk.
     */
    public <U> U withValue(T value, Callable<U> thunk) {
        final T oldValue = threadLocal.get();
        threadLocal.set(value);
        try {
            return thunk.call();
        } catch (Exception e) {
            throw ThrowableUtils.sneakyThrow(e);
        } finally {
            threadLocal.set(oldValue);
        }
    }

    /**
     * Retrieve the current value.
     */
    public T getValue() {
        return threadLocal.get();
    }

    /**
     * Change the currently bound value, discarding the old value. Usually withValue() gives better semantics.
     */
    public void setValue(T value) {
        threadLocal.set(value);
    }

    @Override
    public String toString() {
        return "DynamicVariable(" + getValue() + ")";
    }
}
