package com.cheddarflow.dao;

import com.cheddarflow.jdbc.JdbcTemplates;
import com.cheddarflow.model.ImmutableOptionsContract;
import com.cheddarflow.model.ImmutablePowerAlert;
import com.cheddarflow.model.OptionType;
import com.cheddarflow.model.OptionsContract;
import com.cheddarflow.model.PowerAlert;

import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Repository;

@Repository
public class PowerAlertTable extends AbstractDAO<PowerAlert> implements PowerAlertDAO {

    private static final String INSERT_SQL = "insert into power_alerts (id, symbol, alertDate, createdOn, updatedOn, "
      + "contractExpiration, contractStrike, contractType, broken, strength, strengthIncrease, firstSpot, spot, firstVolume, volumeDelta, "
      + "numCalls, numUnusual, numHighlyUnusual, numDarkPool) values (null, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    private static final String UPDATE_SQL = "update power_alerts set updatedOn = ?, contractExpiration = ?, contractStrike = ?, "
      + "contractType = ?, broken = ?, strength = ?, strengthIncrease = ?, firstSpot = ?, spot = ?, firstVolume = ?, volumeDelta = ?, "
      + "numCalls = ?, numUnusual = ?, numHighlyUnusual = ?, numDarkPool = ? where id = ?";

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
          .spot(rs.getFloat("spot"))
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
            JdbcTemplates.getInstance().getTemplate(false).update(INSERT_SQL, params);
        } else {
            final Object[] params = this.getUpdateParams(powerAlert);
            JdbcTemplates.getInstance().getTemplate(false).update(INSERT_SQL, params);
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
          pa.getSpot(),
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
          pa.getUpdatedOn(),
          pa.getActiveContract().map(OptionsContract::getExpiration).orElse(null),
          pa.getActiveContract().map(OptionsContract::getStrike).orElse(null),
          pa.getActiveContract().map(OptionsContract::getType).map(OptionType::toDbString).orElse(null),
          pa.isBroken(),
          pa.getStrength(),
          pa.getStrengthIncrease().orElse(null),
          pa.getFirstSpot(),
          pa.getSpot(),
          pa.getFirstVolume().orElse(null),
          pa.getVolumeDelta().orElse(null),
          pa.getNumCalls(),
          pa.getNumUnusual().orElse(null),
          pa.getNumHighlyUnusual().orElse(null),
          pa.getNumDarkPool().orElse(null),
          pa.getId().orElse(0L)
        };
    }
}
