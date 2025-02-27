/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.model.lineage;


import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.atlas.AtlasConfiguration;

import java.io.Serializable;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;

@JsonAutoDetect(getterVisibility = PUBLIC_ONLY, setterVisibility = PUBLIC_ONLY, fieldVisibility = NONE)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
/**
 * This is the root class representing the input for lineage search on-demand.
 */
public class LineageOnDemandConstraints extends LineageOnDemandBaseParams implements Serializable {
    private static final long serialVersionUID = 1L;

    private AtlasLineageOnDemandInfo.LineageDirection direction;
    private int              depth;
    private int              from;

    private static final int LINEAGE_ON_DEMAND_DEFAULT_NODE_COUNT = AtlasConfiguration.LINEAGE_ON_DEMAND_DEFAULT_NODE_COUNT.getInt();
    private static final int LINEAGE_ON_DEMAND_DEFAULT_DEPTH      = 3;

    public LineageOnDemandConstraints() {
        this(AtlasLineageOnDemandInfo.LineageDirection.BOTH, -1, -1, LINEAGE_ON_DEMAND_DEFAULT_DEPTH);
    }

    public LineageOnDemandConstraints(LineageOnDemandBaseParams baseParams) {
        this(AtlasLineageOnDemandInfo.LineageDirection.BOTH, baseParams.getInputRelationsLimit(), baseParams.getOutputRelationsLimit(), LINEAGE_ON_DEMAND_DEFAULT_DEPTH);
    }

    public LineageOnDemandConstraints(AtlasLineageOnDemandInfo.LineageDirection direction, LineageOnDemandBaseParams baseParams, int depth) {
        this(direction, baseParams.getInputRelationsLimit(), baseParams.getOutputRelationsLimit(), depth);
    }

    public LineageOnDemandConstraints(AtlasLineageOnDemandInfo.LineageDirection direction, int inputRelationsLimit, int outputRelationsLimit, int depth) {
        super(inputRelationsLimit, outputRelationsLimit);
        this.direction            = direction;
        this.depth                = depth;
    }

    public LineageOnDemandConstraints(AtlasLineageOnDemandInfo.LineageDirection direction, int inputRelationsLimit, int outputRelationsLimit, int depth, int from) {
        super(inputRelationsLimit, outputRelationsLimit);
        this.direction            = direction;
        this.depth                = depth;
        this.from                 = from;
    }

    public AtlasLineageOnDemandInfo.LineageDirection getDirection() {
        return direction;
    }

    public void setDirection(AtlasLineageOnDemandInfo.LineageDirection direction) {
        this.direction = direction;
    }

    public int getDepth() {
        return depth;
    }

    public void setDepth(int depth) {
        this.depth = depth;
    }

    public int getFrom() {
        return from;
    }

    public void setFrom(int from) {
        this.from = from;
    }
}