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
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import org.tisonkun.morax.proto.controller.ServiceInfoProto;
import org.tisonkun.morax.proto.controller.ServiceType;

@ThreadSafe
public class ControllerState implements Serializable {
    @Serial
    private static final long serialVersionUID = 42L;

    private final ReentrantReadWriteLock servicesLock = new ReentrantReadWriteLock();

    @GuardedBy("servicesLock")
    private final Collection<ServiceInfoProto> services = new HashSet<>();

    private final AtomicLong ledgerIdGen = new AtomicLong();

    public Collection<ServiceInfoProto> listServices(List<ServiceType> serviceTypes) {
        servicesLock.readLock().lock();
        try {
            final Set<ServiceInfoProto> result = new HashSet<>();
            for (ServiceInfoProto serviceInfo : services) {
                if (serviceTypes.contains(serviceInfo.getType())) {
                    result.add(serviceInfo);
                }
            }
            return result;
        } finally {
            servicesLock.readLock().unlock();
        }
    }

    public boolean registerService(ServiceInfoProto serviceInfo) {
        servicesLock.writeLock().lock();
        try {
            return services.add(serviceInfo);
        } finally {
            servicesLock.writeLock().unlock();
        }
    }

    public long nextLedgerId() {
        return ledgerIdGen.incrementAndGet();
    }
}
