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

package org.tisonkun.morax.bookie;

/**
 * Storage layer events for logging.
 */
public enum StorageEvent {
    /**
     * EntryLog ID candidates selected. These are the set entry log ID that subsequent entry log files
     * will use. To find the candidates, the Bookie lists all the log IDs which have already been used,
     * and finds the largest contiguous block of free IDs. Over the lifetime of a Bookie, a log ID can
     * be reused. This is not a problem, as the IDs are only referenced from the index, and an
     * entry log file will not be deleted if there are still references to it in the index.
     * Generally candidates are selected at Bookie boot, but they may also be selected at a later time
     * if the current set of candidates is depleted.
     */
    ENTRY_LOG_IDS_CANDIDATES_SELECTED,

    /**
     * The entry logger has started writing a new log file. The previous log file may not
     * be entirely flushed when this is called, though they will be after an explicit flush call.
     */
    LOG_ROLL,
}
