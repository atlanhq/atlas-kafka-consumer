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
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.atlas.AtlasErrorCode;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.SearchParameters;
import org.apache.atlas.model.lineage.AtlasLineageInfo.LineageDirection;
import org.apache.commons.lang.StringUtils;

import java.util.HashSet;
import java.util.Set;

import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE;
import static com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.PUBLIC_ONLY;
import static org.apache.atlas.AtlasErrorCode.BAD_REQUEST;
import static org.apache.atlas.model.lineage.AtlasLineageInfo.LineageDirection.BOTH;


@JsonAutoDetect(getterVisibility = PUBLIC_ONLY, setterVisibility = PUBLIC_ONLY, fieldVisibility = NONE)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public class AtlasLineageRequest {
    private String guid;
    private int depth;
    private int offset = -1;
    private int limit = -1;
    private boolean calculateRemainingVertexCounts;
    private boolean hideProcess;
    private boolean allowDeletedProcess;
    private LineageDirection direction = BOTH;
    private SearchParameters.FilterCriteria entityFilters;

    private Set<String> attributes;
    private Set<String> ignoredProcesses;
    private Set<String> relationAttributes;

    public AtlasLineageRequest() {
    }

    public AtlasLineageRequest(String guid, int depth, LineageDirection direction, boolean hideProcess, int offset, int limit, boolean calculateRemainingVertexCounts) throws AtlasBaseException {
        this.guid = guid;
        this.depth = depth;
        this.direction = direction;
        this.hideProcess = hideProcess;
        this.offset = offset;
        this.limit = limit;
        this.calculateRemainingVertexCounts = calculateRemainingVertexCounts;
        this.attributes = new HashSet<>();
        this.ignoredProcesses = new HashSet<>();
        performValidation();
    }

    public void performValidation() throws AtlasBaseException {
        if (StringUtils.isEmpty(guid)) {
            throw new AtlasBaseException(BAD_REQUEST, "guid is not specified");
        } else if ((offset != -1 && limit == -1) || (offset == -1 && limit != -1)) {
            throw new AtlasBaseException(AtlasErrorCode.INVALID_PAGINATION_STATE);
        } else if (depth != 1 && offset != -1) {
            throw new AtlasBaseException(AtlasErrorCode.PAGINATION_CAN_ONLY_BE_USED_WITH_DEPTH_ONE);
        } else if (offset == -1 && calculateRemainingVertexCounts) {
            throw new AtlasBaseException(AtlasErrorCode.CANT_CALCULATE_VERTEX_COUNTS_WITHOUT_PAGINATION);
        }
    }

    public String getGuid() {
        return guid;
    }

    public void setGuid(String guid) {
        this.guid = guid;
    }

    public int getDepth() {
        return depth;
    }

    public void setDepth(int depth) {
        this.depth = depth;
    }

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public LineageDirection getDirection() {
        return direction;
    }

    public void setDirection(LineageDirection direction) {
        this.direction = direction;
    }

    public boolean isHideProcess() {
        return hideProcess;
    }

    public void setHideProcess(boolean hideProcess) {
        this.hideProcess = hideProcess;
    }

    public Set<String> getAttributes() {
        return attributes;
    }

    public void setAttributes(Set<String> attributes) {
        this.attributes = attributes;
    }

    public Set<String> getIgnoredProcesses() {
        return ignoredProcesses;
    }

    public void setIgnoredProcesses(Set<String> ignoredProcesses) {
        this.ignoredProcesses = ignoredProcesses;
    }

    public SearchParameters.FilterCriteria getEntityFilters() {
        return entityFilters;
    }

    public void setEntityFilters(SearchParameters.FilterCriteria entityFilters) {
        this.entityFilters = entityFilters;
    }

    public boolean isAllowDeletedProcess() {
        return allowDeletedProcess;
    }

    public void setAllowDeletedProcess(boolean allowDeletedProcess) {
        this.allowDeletedProcess = allowDeletedProcess;
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public boolean getCalculateRemainingVertexCounts() {
        return calculateRemainingVertexCounts;
    }

    public void setCalculateRemainingVertexCounts(boolean calculateRemainingVertexCounts) {
        this.calculateRemainingVertexCounts = calculateRemainingVertexCounts;
    }

    public Set<String> getRelationAttributes() {
        return relationAttributes;
    }

    public void setRelationAttributes(Set<String> relationAttributes) {
        this.relationAttributes = relationAttributes;
    }
}
