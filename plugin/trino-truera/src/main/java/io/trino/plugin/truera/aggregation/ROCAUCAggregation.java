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
package io.trino.plugin.truera.aggregation;

import io.trino.plugin.truera.state.AreaUnderRocCurveState;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.*;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.StandardTypes;

@AggregationFunction("roc_auc")
public class ROCAUCAggregation {
  @InputFunction
  public static void input(AreaUnderRocCurveState state, @SqlType(StandardTypes.DOUBLE) double score, @SqlType(StandardTypes.BOOLEAN) boolean label) {
      GroupedRocAucCurve auc = state.get();
      BlockBuilder labelBlockBuilder = BooleanType.BOOLEAN.createBlockBuilder(null, 0);
      BooleanType.BOOLEAN.writeBoolean(labelBlockBuilder, label);
      BlockBuilder scoreBlockBuilder = DoubleType.DOUBLE.createBlockBuilder(null, 0);
      DoubleType.DOUBLE.writeDouble(scoreBlockBuilder, score);

      long startSize = auc.estimatedInMemorySize();
      auc.add(labelBlockBuilder.build(), scoreBlockBuilder.build(), 0, 0);
      state.addMemoryUsage(auc.estimatedInMemorySize() - startSize);
  }

  @CombineFunction
  public static void combine(AreaUnderRocCurveState state, AreaUnderRocCurveState otherState) {
      if (!state.get().isCurrentGroupEmpty() && !otherState.get().isCurrentGroupEmpty()) {
          GroupedRocAucCurve auc = state.get();
          long startSize = auc.estimatedInMemorySize();
          auc.addAll(otherState.get());
          state.addMemoryUsage(auc.estimatedInMemorySize() - startSize);
      }
      else if (state.get().isCurrentGroupEmpty()) {
          state.set(otherState.get());
      }
  }

  @OutputFunction(StandardTypes.DOUBLE)
  public static void output(AreaUnderRocCurveState state, BlockBuilder out) {
      GroupedRocAucCurve auc = state.get();
      auc.serialize(out);
  }
}
