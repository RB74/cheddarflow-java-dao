package com.cheddarflow.dao;

import com.cheddarflow.model.MarketData;
import com.cheddarflow.model.MarketDataInput;
import com.cheddarflow.model.PutCallSummary;

import java.util.Date;
import java.util.List;

public interface TradesDAO {

    PutCallSummary getPutCallSummary(Date from, Date to, String symbolSearch);
    MarketData getMarketData(Date date, int tradeid);
    List<MarketData> getMarketData(Date from, Date to, String symbol, int limit);
    void setMarketData(MarketDataInput in);
    long getMaxTimestamp();
}
