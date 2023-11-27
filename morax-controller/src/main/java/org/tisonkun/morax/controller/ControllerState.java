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

package org.tisonkun.morax.controller;

import java.io.Serial;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import org.tisonkun.morax.proto.controller.ServiceProto;

@ThreadSafe
public class ControllerState implements Serializable {
    @Serial
    private static final long serialVersionUID = 42L;

    private final ReentrantReadWriteLock bookiesLock = new ReentrantReadWriteLock();

    @GuardedBy("bookiesLock")
    private final Collection<ServiceProto> bookies = new HashSet<>();

    private final AtomicLong ledgerIdGen = new AtomicLong();

    public Collection<ServiceProto> listBookies() {
        bookiesLock.readLock().lock();
        try {
            return new HashSet<>(bookies);
        } finally {
            bookiesLock.readLock().unlock();
        }
    }

    public boolean registerBookie(ServiceProto service) {
        bookiesLock.writeLock().lock();
        try {
            return bookies.add(service);
        } finally {
            bookiesLock.writeLock().unlock();
        }
    }

    public long nextLedgerId() {
        return ledgerIdGen.incrementAndGet();
    }
}
