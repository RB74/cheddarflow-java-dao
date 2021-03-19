package com.cheddarflow.dao;

import com.cheddarflow.model.PowerAlert;

import java.util.Date;
import java.util.List;
import java.util.Optional;

public interface PowerAlertDAO {

    List<PowerAlert> findBySymbol(String symbol);
    Optional<PowerAlert> findBySymbolAndDate(String symbol, Date alertDate);
    void save(PowerAlert powerAlert);
    void bulkInsert(List<PowerAlert> powerAlerts);
    void bulkUpdate(List<PowerAlert> powerAlerts);
}
