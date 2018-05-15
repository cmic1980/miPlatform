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
import java.time.*;
import java.time.temporal.TemporalField;
import java.util.*;

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
            calendar.add(Calendar.MONTH, 1);
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

        aggregationResult.getItemList().forEach(s -> {
            s.put("start", start);
            s.put("end", end);
        });

        // Daily 集合名称
        String dailyCollectionName = aggregationParameter.getDailyCubeCoCollectionName();
        var dailyCollection = template.getDb().getCollection(dailyCollectionName);
        if (aggregationResult.getItemList() != null && aggregationResult.getItemList().size() != 0) {
            dailyCollection.insertMany(aggregationResult.getItemList());
        }

    }

    private void generateCubeForMonthly(AggregationParameter aggregationParameter, Date day) throws ParseException {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(day);
        calendar.set(Calendar.DAY_OF_MONTH, 1);
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
        aggregationResult.getItemList().forEach(s -> {
            s.put("start", start);
            s.put("end", end);
        });

        // Daily 集合名称
        String monthlyCubeCoCollectionName = aggregationParameter.getMonthlyCubeCoCollectionName();
        var monthlyCubeCoCollection = template.getDb().getCollection(monthlyCubeCoCollectionName);
        if (aggregationResult.getItemList() != null && aggregationResult.getItemList().size() != 0) {
            monthlyCubeCoCollection.insertMany(aggregationResult.getItemList());
        }
    }

    @Override
    public AggregationResult aggregateFromCube(AggregationParameter aggregationParameter) throws ParseException {
        ZoneId utc = ZoneId.systemDefault();

        String collectionName = aggregationParameter.getCollectionName();
        Group groupParameter = aggregationParameter.getGroup();

        TimeCondition timeCondition = aggregationParameter.getTimeCondition();
        var start = timeCondition.getStart();
        var end = timeCondition.getEnd();

        // 查询数据从 monthly cube 及明细， 时间分段 其实时间后一个月1号前事件 + 月 cube，截至时间当月明细
        // 2014-02-08 <= t < 2015-03-10 => ( 2014-02-08 <= t1 2014-03-01 (daily), 2014-03-01<= t2 < 2015-03-01 (monthly), 2015-03-01<=t3<2015-03-10 (daily))

        // 判断其实时间和截至时间是否在一个月内数据量比较小不用分段计算
        var localStart = start.toInstant().atZone(utc).toLocalDate();
        var localEnd = end.toInstant().atZone(utc).toLocalDate();
        List<Match> matchList = null;
        var period = Period.between(localStart, localEnd);
        if (period.getYears() == 0 && period.getMonths() == 0) {
            matchList = aggregationParameter.getMatchList();

            Match match;
            match = new Match();
            match.setValue(start);
            match.setOperate(MatchOperate.gte);
            match.setField(timeCondition.getField());
            match.setLink(MatchLinkOperate.and);
            matchList.add(match);

            match = new Match();
            match.setValue(end);
            match.setOperate(MatchOperate.lt);
            match.setField(timeCondition.getField());
            matchList.add(match);

            AggregationResult aggregationResult = this.aggregate(collectionName, groupParameter, matchList);
            return aggregationResult;
        } else {
            AggregationResult aggregationResult = null;


            /**
             * t1
             * */

            var localTimeStart = start.toInstant().atZone(utc).toLocalDateTime();
            ZonedDateTime zdtT1Start = localTimeStart.atZone(utc);

            //获取年
            Integer yearStart = localTimeStart.getYear();
            //获取月份，0表示1月份
            Integer monthStart = localTimeStart.getMonthValue();
            //获取当前天数
            Integer dayStart = localTimeStart.getDayOfMonth();
            //获取当前小时
            Integer hourStart = localTimeStart.getHour();
            //获取当前分钟
            Integer minuteStart = localTimeStart.getMinute();
            //获取当前秒
            Integer secondStart = localTimeStart.getSecond();


            // 如果是本月第一天忽略t1
            Date t1Start = start;
            Date t1End = start;

            if (dayStart.equals(1) == true && hourStart.equals(0) && minuteStart.equals(0) && secondStart.equals(0)) {

            } else {
                matchList = new ArrayList<>();
                matchList.addAll(aggregationParameter.getMatchList());

                // t1-> 从开始时间到下个月一号零时零分
                LocalDate t1LocalEnd = LocalDate.of(yearStart, monthStart, 1);
                t1LocalEnd = t1LocalEnd.plusMonths(1);
                ZonedDateTime zdtT1End = t1LocalEnd.atStartOfDay(utc);
                t1End = Date.from(zdtT1End.toInstant());

                Match match;
                match = new Match();
                match.setValue(t1Start);
                match.setOperate(MatchOperate.gte);
                match.setField(timeCondition.getField());
                match.setLink(MatchLinkOperate.and);
                matchList.add(match);

                match = new Match();
                match.setValue(t1End);
                match.setOperate(MatchOperate.lt);
                match.setField(timeCondition.getField());
                matchList.add(match);

                var t1AggregationResult = this.aggregate(collectionName, groupParameter, matchList);
                aggregationResult = t1AggregationResult;
            }


            /**
             * t2
             * */
            // t2-> 从开始时间到下个月一号零时零分为第二阶段为
            var localTimeEnd = end.toInstant().atZone(utc).toLocalDateTime();
            //获取年
            Integer yearEnd = localTimeEnd.getYear();
            //获取月份，0表示1月份
            Integer monthEnd = localTimeEnd.getMonthValue();
            //获取当前天数
            Integer dayEnd = localTimeEnd.getDayOfMonth();
            //获取当前小时
            Integer hourEnd = localTimeEnd.getHour();
            //获取当前分钟
            Integer minuteEnd = localTimeEnd.getMinute();
            //获取当前秒
            Integer secondEnd = localTimeEnd.getSecond();

            LocalDate t2LocalEnd = LocalDate.of(yearEnd, monthEnd, 1);
            ZonedDateTime zdtT2End = t2LocalEnd.atStartOfDay(utc);
            var t2End = Date.from(zdtT2End.toInstant());

            matchList = new ArrayList<>();
            matchList.addAll(aggregationParameter.getMatchList());

            Match match;
            match = new Match();
            match.setValue(t1End);
            match.setOperate(MatchOperate.gte);
            match.setField("start");
            match.setLink(MatchLinkOperate.and);
            matchList.add(match);

            match = new Match();
            match.setValue(t2End);
            match.setOperate(MatchOperate.lt);
            match.setField("end");
            matchList.add(match);

            String monthlyCollectionName = aggregationParameter.getMonthlyCubeCoCollectionName();
            // LocalDateTime s1 = LocalDateTime.now();
            var t2AggregationResult = this.aggregate(monthlyCollectionName, groupParameter, matchList);
            // LocalDateTime e1 = LocalDateTime.now();
            // var se1 = Duration.between(s1,e1);

            /*
            matchList = new ArrayList<>();
            matchList.addAll(aggregationParameter.getMatchList());
            match = new Match();
            match.setValue(t1End);
            match.setOperate(MatchOperate.gte);
            match.setField(timeCondition.getField());
            match.setLink(MatchLinkOperate.and);
            matchList.add(match);

            match = new Match();
            match.setValue(t2End);
            match.setOperate(MatchOperate.lt);
            match.setField(timeCondition.getField());
            matchList.add(match);

            LocalDateTime s2 = LocalDateTime.now();
            var t3AggregationResult = this.aggregate(collectionName, groupParameter, matchList);
            LocalDateTime e2 = LocalDateTime.now();
            var se2 = Duration.between(s2,e2);
            */

            if (aggregationResult == null) {
                aggregationResult = t2AggregationResult;
            } else {
                aggregationResult.merge(t2AggregationResult);
            }

            // 第一段时间 2014-01-01
            return aggregationResult;
        }
    }

    @Override
    public AggregationResult aggregate(AggregationParameter aggregationParameter) throws ParseException {
        String collectionName = aggregationParameter.getCollectionName();
        var groupParameter = aggregationParameter.getGroup();
        var timeCondition = aggregationParameter.getTimeCondition();
        List<Match> matchList = new ArrayList<>();
        matchList.addAll(aggregationParameter.getMatchList());

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

        var aggregationResult = this.aggregate(collectionName, groupParameter, matchList);
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

            if (linkOperate == null) { // 无连接表达式
                if (criteria == null) // 无条件第一个表达式
                {
                    criteria = iCriteria;
                } else {
                    criteria = criteria.andOperator(iCriteria);
                }
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
