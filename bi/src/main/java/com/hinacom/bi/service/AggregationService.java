package com.hinacom.bi.service;

import com.hinacom.bi.domain.AggregationParameter;
import com.hinacom.bi.domain.AggregationResult;

public interface AggregationService {
    AggregationResult aggregate(AggregationParameter aggregationParameter);
}
