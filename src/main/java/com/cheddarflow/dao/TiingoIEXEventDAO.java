package com.cheddarflow.dao;

import com.cheddarflow.model.TiingoIEXEvent;

import java.util.Date;
import java.util.List;

public interface TiingoIEXEventDAO {

    List<TiingoIEXEvent> listObjects(Date from, Date to, List<String> symbols);
    List<TiingoIEXEvent> listObjects(Date from, Date to, String symbol, boolean rollback, int limit);
    List<TiingoIEXEvent> mostRecentObjects(List<String> symbols);
    long getMaxTimestamp();
}
