package com.cheddarflow.dao;

import com.cheddarflow.model.PowerAlert;

import java.util.Date;
import java.util.List;
import java.util.Optional;

public interface PowerAlertDAO {

    List<PowerAlert> findBySymbolAndDateRange(String symbol, Date from, Date to, int minStrength, boolean rollback);
    List<PowerAlert> findBySymbol(String symbol);
    Optional<PowerAlert> findBySymbolAndDate(String symbol, Date alertDate);
    void save(PowerAlert powerAlert);
    void bulkInsert(List<PowerAlert> powerAlerts);
    void bulkUpdate(List<PowerAlert> powerAlerts);
    void deleteBefore(Date cutoff);
}
