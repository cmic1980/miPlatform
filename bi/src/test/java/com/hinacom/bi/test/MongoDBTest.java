package com.hinacom.bi.test;

import com.alibaba.fastjson.JSONObject;
import com.hinacom.bi.domain.*;
import com.hinacom.bi.service.AggregationService;
import com.mongodb.BasicDBObject;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.bson.Document;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.Console;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

import static org.springframework.data.mongodb.core.aggregation.Aggregation.*;


@RunWith(SpringRunner.class)
@SpringBootTest
public class MongoDBTest {
    @Autowired
    private AggregationService aggregationService;
    @Autowired
    private MongoOperations operations;

    @Autowired
    private MongoTemplate template;

    @Test
    public void run() throws ParseException, IOException {
        URL url = this.getClass().getResource("/input.json");
        String fileName = url.getFile();
        File file = new File(fileName);

        String json = FileUtils.readFileToString(file,"utf-8");
        AggregationParameter aggregationParameter = JSONObject.parseObject(json, AggregationParameter.class);

        AggregationResult aggregationResultFromCube = aggregationService.aggregateFromCube(aggregationParameter);

        aggregationParameter = JSONObject.parseObject(json, AggregationParameter.class);
        AggregationResult aggregationResult = aggregationService.aggregate(aggregationParameter);

        // aggregationResultFromCube = null;
        aggregationResult = null;
    }

    @Test
    public void testGenerateCube() throws ParseException, IOException {
        URL url = this.getClass().getResource("/input.json");
        String fileName = url.getFile();
        File file = new File(fileName);

        String json = FileUtils.readFileToString(file,"utf-8");
        AggregationParameter aggregationParameter = JSONObject.parseObject(json, AggregationParameter.class);

        aggregationService.generateCube(aggregationParameter, true);
    }
}
