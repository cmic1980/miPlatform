package com.hinacom.bi.domain;

import java.util.List;

public class AggregationParameter {
    private final static String DAILY_CUBE_TAG = "Daily";
    private final static String MONTHLY_CUBE_TAG = "Monthly";


    private String collectionName;
    private TimeCondition timeCondition;
    private Group group;
    private List<Match> matchList;

    public String getCollectionName() {
        return collectionName;
    }

    public void setCollectionName(String collectionName) {
        this.collectionName = collectionName;
    }

    public TimeCondition getTimeCondition() {
        return timeCondition;
    }

    public void setTimeCondition(TimeCondition timeCondition) {
        this.timeCondition = timeCondition;
    }

    public Group getGroup() {
        return group;
    }

    public void setGroup(Group group) {
        this.group = group;
    }

    public List<Match> getMatchList() {
        return matchList;
    }

    public void setMatchList(List<Match> matchList) {
        this.matchList = matchList;
    }

    public String getDailyCubeCoCollectionName()
    {
        String dailyCubeCoCollectionName = this.getCollectionName().concat(this.getGroup().getCube()).concat(DAILY_CUBE_TAG);
        return dailyCubeCoCollectionName;
    }

    public String getMonthlyCubeCoCollectionName()
    {
        String dailyCubeCoCollectionName = this.getCollectionName().concat(this.getGroup().getCube()).concat(MONTHLY_CUBE_TAG);
        return dailyCubeCoCollectionName;
    }
}
