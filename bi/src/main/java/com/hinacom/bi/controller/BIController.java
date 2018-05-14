package com.hinacom.bi.controller;

import com.hinacom.bi.domain.AggregationParameter;
import com.hinacom.bi.domain.AggregationResult;
import com.hinacom.bi.service.AggregationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;

@RestController
public class BIController {
    @Autowired
    private AggregationService aggregationService;

    @RequestMapping(value = "/bi/aggregate", method = RequestMethod.POST)
    public AggregationResult aggregateFromCube(@RequestBody AggregationParameter parameter) throws ParseException {
        var result = aggregationService.aggregateFromCube(parameter);
        return result;
    }
}
