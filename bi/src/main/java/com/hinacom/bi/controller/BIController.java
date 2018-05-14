package com.hinacom.bi.controller;

import com.hinacom.bi.domain.AggregationParameter;
import com.hinacom.bi.domain.AggregationResult;
import com.hinacom.bi.service.AggregationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class BIController {
    @Autowired
    private AggregationService aggregationService;

    @RequestMapping(value = "/bi/aggregate", method = RequestMethod.POST)
    public AggregationResult aggregate(@RequestBody AggregationParameter parameter) {
        var result = aggregationService.aggregate(parameter);
        return result;
    }
}
