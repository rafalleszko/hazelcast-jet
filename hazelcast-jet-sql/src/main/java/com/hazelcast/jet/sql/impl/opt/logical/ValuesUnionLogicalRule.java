/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.opt.logical;

import com.hazelcast.jet.sql.impl.opt.OptUtils;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.logical.LogicalUnion;

import java.util.List;

import static java.util.stream.Collectors.toList;

final class ValuesUnionLogicalRule extends RelOptRule {

    static final RelOptRule INSTANCE = new ValuesUnionLogicalRule();

    private ValuesUnionLogicalRule() {
        super(
                operand(LogicalUnion.class, unordered(operand(ValuesLogicalRel.class, none()))),
                RelFactories.LOGICAL_BUILDER,
                ValuesUnionLogicalRule.class.getSimpleName()
        );
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Union union = call.rel(0);

        List<Object[]> values = extractValues(union);

        ValuesLogicalRel rel = new ValuesLogicalRel(
                union.getCluster(),
                OptUtils.toLogicalConvention(union.getTraitSet()),
                union.getRowType(),
                values
        );
        call.transformTo(rel);
    }

    private static List<Object[]> extractValues(Union union) {
        return union.getInputs().stream()
                    .flatMap(input -> OptUtils.extractRelsFromSubset(input).stream())
                    .filter(input -> input instanceof ValuesLogicalRel)
                    .map(input -> (ValuesLogicalRel) input)
                    .flatMap(values -> values.values().stream())
                    .collect(toList());
    }
}
