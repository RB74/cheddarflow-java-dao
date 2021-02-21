package com.cheddarflow.dao;

import com.cheddarflow.model.DXTimeAndSale;

import java.util.Date;
import java.util.List;

public interface DXTimeAndSaleDAO {

    List<DXTimeAndSale> listObjects(Date from, Date to, String symbol, boolean rollback, int limit);

    void bulkInsert(List<DXTimeAndSale> in);

    long getMaxTimestamp();
}
