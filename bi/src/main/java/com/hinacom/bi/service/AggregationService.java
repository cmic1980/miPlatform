package com.hinacom.bi.service;

import com.hinacom.bi.domain.AggregationParameter;
import com.hinacom.bi.domain.AggregationResult;
import com.hinacom.bi.domain.Group;
import com.hinacom.bi.domain.TimeCondition;

public interface AggregationService {
    /**
     * 从预处理的Cube中汇总数据
     * */
    AggregationResult aggregateFromCube(AggregationParameter aggregationParameter);

    AggregationResult aggregateByTimeScope(String collectionName, Group groupParameter, TimeCondition timeCondition);

    void generateCube(AggregationParameter aggregationParameter, Boolean isReimport);
}
