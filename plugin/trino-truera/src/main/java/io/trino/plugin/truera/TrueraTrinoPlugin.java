/*
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
package io.trino.plugin.truera;

import com.google.common.collect.ImmutableSet;
import io.trino.plugin.truera.metrics.AUCROCAggregate;
import io.trino.plugin.truera.udf.drift.ApproxWassersteinDrift;
import io.trino.plugin.truera.udf.drift.WassersteinDrift;
import io.trino.spi.Plugin;
import java.util.Set;

public class TrueraTrinoPlugin
        implements Plugin
{

    @Override
    public Set<Class<?>> getFunctions()
    {
        return ImmutableSet.<Class<?>>builder()
                .add(AUCROCAggregate.class)
                .add(ApproxWassersteinDrift.class)
                .add(WassersteinDrift.class)
                .build();
    }
}
