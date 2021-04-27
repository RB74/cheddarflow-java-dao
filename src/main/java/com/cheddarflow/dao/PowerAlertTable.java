package com.cheddarflow.dao;

import com.cheddarflow.jdbc.JdbcTemplates;
import com.cheddarflow.jdbc.NoDataInRangeException;
import com.cheddarflow.model.ImmutableOptionsContract;
import com.cheddarflow.model.ImmutablePowerAlert;
import com.cheddarflow.model.OptionType;
import com.cheddarflow.model.OptionsContract;
import com.cheddarflow.model.PowerAlert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Repository;

@Repository
public class PowerAlertTable extends AbstractDAO<PowerAlert> implements PowerAlertDAO {

    private static final String INSERT_SQL = "insert into power_alerts (symbol, alertDate, createdOn, updatedOn, "
      + "contractExpiration, contractStrike, contractType, broken, strength, strengthIncrease, firstSpot, firstVolume, volumeDelta, "
      + "numCalls, numUnusual, numHighlyUnusual, numDarkPool) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    private static final String UPDATE_SQL = "update power_alerts set createdOn = ?, updatedOn = ?, contractExpiration = ?, "
      + "contractStrike = ?, contractType = ?, broken = ?, strength = ?, strengthIncrease = ?, firstSpot = ?, firstVolume = ?, "
      + "volumeDelta = ?, numCalls = ?, numUnusual = ?, numHighlyUnusual = ?, numDarkPool = ? where id = ?";

    private final RowMapper<PowerAlert> mapper = (rs, i) -> {
        final String symbol = rs.getString("symbol");
        final Date contractExp = rs.getDate("contractExpiration");
        final Optional<OptionsContract> contract = contractExp == null ? Optional.empty()
          : Optional.of(ImmutableOptionsContract.builder().expiration(contractExp).strike(rs.getFloat("contractStrike"))
            .type(OptionType.forString(rs.getString("contractType"))).symbol(symbol).build());
        return ImmutablePowerAlert.builder()
          .id(rs.getLong("id"))
          .symbol(symbol)
          .alertDate(rs.getDate("alertDate"))
          .createdOn(rs.getTimestamp("createdOn"))
          .updatedOn(rs.getTimestamp("updatedOn"))
          .firstVolume(Optional.ofNullable((Float)rs.getObject("firstVolume")))
          .volumeDelta(Optional.ofNullable((Float)rs.getObject("volumeDelta")))
          .isBroken(rs.getBoolean("broken"))
          .numCalls(rs.getInt("numCalls"))
          .numUnusual(Optional.ofNullable((Integer)rs.getObject("numUnusual")))
          .numHighlyUnusual(Optional.ofNullable((Integer)rs.getObject("numHighlyUnusual")))
          .numDarkPool(Optional.ofNullable((Integer)rs.getObject("numDarkPool")))
          .firstSpot(rs.getFloat("firstSpot"))
          .strength(rs.getInt("strength"))
          .strengthIncrease(Optional.ofNullable((Integer)rs.getObject("strengthIncrease")))
          .activeContract(contract)
          .build();
    };

    @Autowired
    public PowerAlertTable(@Qualifier("normalTaskExecutor") ThreadPoolTaskExecutor taskExecutor) {
        super(taskExecutor);
    }

    @Override
    public List<PowerAlert> findBySymbolAndDateRange(String symbol, Date from, Date to, int minStrength, boolean rollback) {
        final JdbcTemplate template = JdbcTemplates.getInstance().getTemplate(true);
        final List<PowerAlert> data = this.doFindBySymbolAndDateRange(symbol, from, to, minStrength, template);
        if (!data.isEmpty() || !rollback)
            return data;

        final Date rollbackDate = new Date(from.getTime() - TimeUnit.DAYS.toMillis(7));
        try {
            Date minDate = template.queryForObject("select max(createdOn) from power_alerts"
              + " where createdOn >= ?", new Object[] { rollbackDate }, Date.class);
            if (minDate != null) {
                Calendar c = Calendar.getInstance();
                c.setTime(minDate);
                c.set(Calendar.HOUR_OF_DAY, 0);
                c.set(Calendar.MINUTE, 0);
                c.set(Calendar.SECOND, 0);
                c.set(Calendar.MILLISECOND, 0);
                minDate = c.getTime();
                c.add(Calendar.MILLISECOND, (int)TimeUnit.DAYS.toMillis(1));
                return this.doFindBySymbolAndDateRange(symbol, minDate, c.getTime(), minStrength, template);
            } else {
                throw new NoDataInRangeException("No data found");
            }
        } catch (EmptyResultDataAccessException e) {
            throw new NoDataInRangeException("No data found");
        }
    }

    private List<PowerAlert> doFindBySymbolAndDateRange(String symbol, Date from, Date to, int minStrength, JdbcTemplate template) {
        final List<Object> params = new ArrayList<>(5);
        params.add(from);
        params.add(to);
        params.add(minStrength);
        params.add(minStrength >= 35 ? 4 : 3);
        String sql = "select * from power_alerts where alertDate between ? and ? and strength >= ? and numCalls >= ?";
        if (symbol != null && !symbol.isBlank()) {
            sql += " and symbol = ?";
            params.add(symbol);
        }
        return template.query(sql, this.mapper, params.toArray(new Object[0]));
    }

    @Override
    public List<PowerAlert> findBySymbol(String symbol) {
        return JdbcTemplates.getInstance().getTemplate(true).query("select * from power_alerts where symbol = ?",
          this.mapper, symbol);
    }

    @Override
    public Optional<PowerAlert> findBySymbolAndDate(String symbol, Date alertDate) {
        return JdbcTemplates.getInstance().getTemplate(true)
          .query("select * from power_alerts where symbol = ? and alertDate = ? limit 1", this.mapper, symbol, alertDate)
          .stream().findFirst();
    }

    @Override
    public void save(PowerAlert powerAlert) {
        if (powerAlert.getId().isEmpty()) {
            final Object[] params = this.getInsertParams(powerAlert);
            logger.trace("Insert params: {}", Arrays.toString(params));
            JdbcTemplates.getInstance().getTemplate(false).update(INSERT_SQL, params);
        } else {
            final Object[] params = this.getUpdateParams(powerAlert);
            logger.trace("Update params: {}", Arrays.toString(params));
            JdbcTemplates.getInstance().getTemplate(false).update(UPDATE_SQL, params);
        }
    }

    @Override
    public void bulkInsert(List<PowerAlert> powerAlerts) {
        final List<Object[]> params = powerAlerts.stream().map(this::getInsertParams).collect(Collectors.toList());
        JdbcTemplates.getInstance().getTemplate(false).batchUpdate(INSERT_SQL, params);
    }

    private Object[] getInsertParams(PowerAlert pa) {
        return new Object[] {
          pa.getSymbol(),
          pa.getAlertDate(),
          pa.getCreatedOn(),
          pa.getUpdatedOn(),
          pa.getActiveContract().map(OptionsContract::getExpiration).orElse(null),
          pa.getActiveContract().map(OptionsContract::getStrike).orElse(null),
          pa.getActiveContract().map(OptionsContract::getType).map(OptionType::toDbString).orElse(null),
          pa.isBroken(),
          pa.getStrength(),
          pa.getStrengthIncrease().orElse(0),
          pa.getFirstSpot(),
          pa.getFirstVolume().orElse(0f),
          pa.getVolumeDelta().orElse(0f),
          pa.getNumCalls(),
          pa.getNumUnusual().orElse(0),
          pa.getNumHighlyUnusual().orElse(0),
          pa.getNumDarkPool().orElse(0)
        };
    }

    @Override
    public void bulkUpdate(List<PowerAlert> powerAlerts) {
        final List<Object[]> params = powerAlerts.stream().map(this::getUpdateParams).collect(Collectors.toList());
        JdbcTemplates.getInstance().getTemplate(false).batchUpdate(UPDATE_SQL, params);
    }

    private Object[] getUpdateParams(PowerAlert pa) {
        return new Object[] {
          pa.getCreatedOn(),
          pa.getUpdatedOn(),
          pa.getActiveContract().map(OptionsContract::getExpiration).orElse(null),
          pa.getActiveContract().map(OptionsContract::getStrike).orElse(null),
          pa.getActiveContract().map(OptionsContract::getType).map(OptionType::toDbString).orElse(null),
          pa.isBroken(),
          pa.getStrength(),
          pa.getStrengthIncrease().orElse(0),
          pa.getFirstSpot(),
          pa.getFirstVolume().orElse(0f),
          pa.getVolumeDelta().orElse(0f),
          pa.getNumCalls(),
          pa.getNumUnusual().orElse(0),
          pa.getNumHighlyUnusual().orElse(0),
          pa.getNumDarkPool().orElse(0),
          pa.getId().orElse(0L)
        };
    }
}
