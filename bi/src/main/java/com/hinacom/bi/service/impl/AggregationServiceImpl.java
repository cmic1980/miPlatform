package com.hinacom.bi.service.impl;

import com.hinacom.bi.domain.*;
import com.hinacom.bi.service.AggregationService;
import com.mongodb.BasicDBObject;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.aggregation.MatchOperation;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.stereotype.Service;


import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import static org.springframework.data.mongodb.core.aggregation.Aggregation.group;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.match;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.newAggregation;

@Service
public class AggregationServiceImpl implements AggregationService {
    @Autowired
    private MongoTemplate template;

    private AggregationResult aggregate(String collectionName, Group groupParameter, List<Match> matchList) throws ParseException {
        List<AggregationOperation> aggregationOperationList = new ArrayList<AggregationOperation>();

        // matchs
        Criteria criteria = this.buildCriteria(matchList);
        if (criteria != null) {
            var match = match(criteria);
            aggregationOperationList.add(match);
        }

        // group
        String[] groupFields = groupParameter.getFields();
        // group - fields
        var group = group(groupFields);
        // group - operations
        List<GroupOperation> operationList = groupParameter.getOperationList();
        for (GroupOperation operation : operationList) {
            String alias = "";
            if (StringUtils.isBlank(operation.getAlias())) {
                alias = operation.getField();
            } else {
                alias = operation.getAlias();
            }

            switch (operation.getType()) {
                case count:
                    group = group.count().as(alias);
                    break;
                case sum:
                    group = group.sum(operation.getField()).as(alias);
                    break;
                case avg:
                    group = group.avg(operation.getField()).as(alias);
                    break;
                case min:
                    group = group.min(operation.getField()).as(alias);
                    break;
                case max:
                    group = group.max(operation.getField()).as(alias);
                    break;
            }
        }
        aggregationOperationList.add(group);

        AggregationOperation[] aggregationOperationArray = new AggregationOperation[aggregationOperationList.size()];
        aggregationOperationList.toArray(aggregationOperationArray);

        Aggregation agg = newAggregation(aggregationOperationArray);
        AggregationResults<BasicDBObject> aggregationResults = template.aggregate(agg, collectionName, BasicDBObject.class);
        AggregationResult aggregationResult = AggregationResult.parse(aggregationResults);
        return aggregationResult;
    }

    public AggregationResult aggregateByTimeScope(String collectionName, Group groupParameter, TimeCondition timeCondition) throws ParseException {
        List<Match> matchList = new ArrayList<Match>();
        Match match = new Match();
        match.setValue(timeCondition.getStart());
        match.setOperate(MatchOperate.gt);
        match.setField(timeCondition.getField());
        matchList.add(match);

        AggregationResult aggregationResult = this.aggregate(collectionName, groupParameter, matchList);
        return aggregationResult;
    }

    @Override
    public void generateCube(AggregationParameter aggregationParameter, Boolean isReimport) throws ParseException {

        // 预处理数据
        if (isReimport) {
            // daily
            // 删除daily预处理集合
            String dailyCollectionName = aggregationParameter.getDailyCubeCoCollectionName();
            this.template.getDb().getCollection(dailyCollectionName).drop();
            this.template.getDb().createCollection(dailyCollectionName);

            // 重新插入daily预处理集合
            TimeCondition timeCondition = aggregationParameter.getTimeCondition();
            Date start = timeCondition.getStart();
            Date end = timeCondition.getEnd();
            Calendar calendar;
            while (start.getTime() < end.getTime()) {
                this.generateCubeForDaily(aggregationParameter, start);
                calendar = Calendar.getInstance();
                calendar.setTime(start);
                calendar.add(Calendar.DATE, 1);
                start = calendar.getTime();
            }

            // monthly
            String monthlyCollectionName = aggregationParameter.getMonthlyCubeCoCollectionName();
            this.template.getDb().getCollection(monthlyCollectionName).drop();
            this.template.getDb().createCollection(monthlyCollectionName);
            start = timeCondition.getStart();


            end = timeCondition.getEnd();
            calendar = Calendar.getInstance();
            calendar.setTime(end);
            calendar.add(Calendar.MONTH,1);
            end = calendar.getTime();

            while (start.getTime() < end.getTime()) {
                this.generateCubeForMonthly(aggregationParameter, start);
                calendar = Calendar.getInstance();
                calendar.setTime(start);
                calendar.add(Calendar.MONTH, 1);
                start = calendar.getTime();
            }

        } else {
            ZoneId zoneId = ZoneId.systemDefault();
            LocalDate lday = LocalDate.now();
            ZonedDateTime zdt = lday.atStartOfDay(zoneId);
            Date day = Date.from(zdt.toInstant());
            // daily
            this.generateCubeForDaily(aggregationParameter, day);

            // monthly
            this.generateCubeForMonthly(aggregationParameter, day);
        }

    }

    private void generateCubeForDaily(AggregationParameter aggregationParameter, Date day) throws ParseException {
        Date start = day;
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(start);
        calendar.add(Calendar.DATE, 1);
        Date end = calendar.getTime();

        List<Match> matchList = new ArrayList<Match>();
        Match match = new Match();
        match.setValue(start);
        match.setOperate(MatchOperate.gte);
        match.setField(aggregationParameter.getTimeCondition().getField());
        match.setLink(MatchLinkOperate.and);
        matchList.add(match);

        match = new Match();
        match.setValue(end);
        match.setOperate(MatchOperate.lt);
        match.setField(aggregationParameter.getTimeCondition().getField());
        matchList.add(match);


        // 集合名称
        String collectionName = aggregationParameter.getCollectionName();
        AggregationResult aggregationResult = this.aggregate(collectionName, aggregationParameter.getGroup(), matchList);

        aggregationResult.getItemList().forEach(s->{
            s.put("start",start);
            s.put("end",end);
        });

        // Daily 集合名称
        String dailyCollectionName = aggregationParameter.getDailyCubeCoCollectionName();
        var dailyCollection = template.getDb().getCollection(dailyCollectionName);
        if(aggregationResult.getItemList()!=null && aggregationResult.getItemList().size()!=0)
        {
            dailyCollection.insertMany(aggregationResult.getItemList());
        }

    }

    private void generateCubeForMonthly(AggregationParameter aggregationParameter, Date day) throws ParseException {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(day);
        calendar.set(Calendar.DAY_OF_MONTH,1);
        Date start = calendar.getTime();

        calendar.add(Calendar.MONTH, 1);
        Date end = calendar.getTime();

        List<Match> matchList = new ArrayList<Match>();
        Match match = new Match();
        match.setValue(start);
        match.setOperate(MatchOperate.gte);
        match.setField(aggregationParameter.getTimeCondition().getField());
        match.setLink(MatchLinkOperate.and);
        matchList.add(match);

        match = new Match();
        match.setValue(end);
        match.setOperate(MatchOperate.lt);
        match.setField(aggregationParameter.getTimeCondition().getField());
        matchList.add(match);

        // 集合名称
        String collectionName = aggregationParameter.getCollectionName();
        AggregationResult aggregationResult = this.aggregate(collectionName, aggregationParameter.getGroup(), matchList);
        aggregationResult.getItemList().forEach(s->{
            s.put("start",start);
            s.put("end",end);
        });

        // Daily 集合名称
        String monthlyCubeCoCollectionName = aggregationParameter.getMonthlyCubeCoCollectionName();
        var monthlyCubeCoCollection = template.getDb().getCollection(monthlyCubeCoCollectionName);
        if(aggregationResult.getItemList()!=null && aggregationResult.getItemList().size()!=0) {
            monthlyCubeCoCollection.insertMany(aggregationResult.getItemList());
        }
    }





    @Override
    public AggregationResult aggregateFromCube(AggregationParameter aggregationParameter) throws ParseException {
        String collectionName = aggregationParameter.getCollectionName();
        Group groupParameter = aggregationParameter.getGroup();
        List<Match> matchList = aggregationParameter.getMatchList();

        TimeCondition timeCondition = aggregationParameter.getTimeCondition();
        Match match = new Match();
        match.setValue(timeCondition.getStart());
        match.setOperate(MatchOperate.gte);
        match.setField(timeCondition.getField());
        match.setLink(MatchLinkOperate.and);
        matchList.add(match);

        match = new Match();
        match.setValue(timeCondition.getEnd());
        match.setOperate(MatchOperate.lt);
        match.setField(timeCondition.getField());
        matchList.add(match);

        AggregationResult aggregationResult = this.aggregate(collectionName, groupParameter, matchList);
        return aggregationResult;
    }


    private Criteria buildCriteria(List<Match> matchList) throws ParseException {
        Criteria criteria = null;
        MatchLinkOperate linkOperate = null;
        for (Match match : matchList) {
            String field = match.getField();
            Criteria iCriteria = org.springframework.data.mongodb.core.query.Criteria.where(field);

            switch (match.getOperate()) {
                case lt:
                    iCriteria.lt(match.getObjectValue());
                    break;
                case lte:
                    iCriteria.lte(match.getObjectValue());
                    break;
                case gt:
                    iCriteria.gt(match.getObjectValue());
                    break;
                case gte:
                    iCriteria.gte(match.getObjectValue());
                    break;
                case in:
                    iCriteria.in(match.getObjectValue());
                    break;
            }

            if (linkOperate == null) {
                criteria = iCriteria;
            } else {
                switch (linkOperate) {
                    case and:
                        criteria = criteria.andOperator(iCriteria);
                        break;
                    case or:
                        criteria = criteria.orOperator(iCriteria);
                        break;
                }
            }

            if (match.getLink() != null) {
                linkOperate = match.getLink();
            }
        }
        return criteria;
    }
}
