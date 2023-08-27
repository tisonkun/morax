/*
 * Copyright (c) 2023 tison <wander4096@gmail.com>
 * All rights reserved.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package org.tisonkun.nymph.bookie;

import com.google.common.base.Preconditions;
import java.util.regex.Pattern;

public record BookieId(String id) {
    private static final Pattern BOOKIE_ID_PATTERN = Pattern.compile("[a-zA-Z0-9:-_.\\-]+");

    public BookieId {
        Preconditions.checkNotNull(id, "BookieID is null");
        Preconditions.checkArgument(BOOKIE_ID_PATTERN.matcher(id).matches(), "BookieID %s is invalid", id);
        Preconditions.checkArgument(!"readonly".equalsIgnoreCase(id), "BookieID %s is invalid", id);
    }
}
