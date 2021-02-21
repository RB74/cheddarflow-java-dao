package com.cheddarflow.dao;

import com.cheddarflow.jdbc.JdbcTemplates;
import com.cheddarflow.jdbc.NoDataInRangeException;
import com.cheddarflow.model.DXTimeAndSale;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.TimeZone;
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
public class DXTimeAndSaleTable extends AbstractDAO<DXTimeAndSale> implements DXTimeAndSaleDAO {

    protected final RowMapper<DXTimeAndSale> rowMapper = (rs, i) -> DXTimeAndSale.newBuilder()
      .withId(rs.getLong("id"))
      .withSymbol(rs.getString("symbol"))
      .withIndex(rs.getLong("tradeIndex"))
      .withCreatedOn(rs.getTimestamp("createdOn"))
      .withReceivedOn(rs.getTimestamp("receivedOn"))
      .withSize(rs.getDouble("size"))
      .withExchangeCode(rs.getString("exchangeCode"))
      .withPrice(rs.getDouble("price"))
      .withBidPrice(rs.getDouble("bidPrice"))
      .withAskPrice(rs.getDouble("askPrice"))
      .withExchangeSaleConditions(rs.getString("exchangeSaleConditions"))
      .withAggressorSide(rs.getString("aggressorSide"))
      .withSpreadLeg(rs.getInt("spreadLeg") == 1)
      .withExtendedTradingHours(rs.getInt("extendedTradingHours") == 1)
      .withValidTick(rs.getInt("validTick") == 1)
      .withType(rs.getString("type"))
      .withLateSignature(rs.getInt("lateSignature") == 1)
      .withTradeThroughExempt(rs.getString("tradeThroughExempt"))
      .withSignaturePrint(rs.getInt("signaturePrint"))
      .build();

    @Autowired
    public DXTimeAndSaleTable(@Qualifier("normalTaskExecutor") ThreadPoolTaskExecutor taskExecutor) {
        super(taskExecutor);
    }

    @Override
    public List<DXTimeAndSale> listObjects(Date from, Date to, String symbol, boolean rollback, int limit) {
        if (from.equals(to)) {
            to = new Date(to.getTime() + TimeUnit.DAYS.toMillis(1));
        }

        final JdbcTemplate template = JdbcTemplates.getInstance().getTemplate(true);
        final List<DXTimeAndSale> data = this.doListObjects(symbol, from, to, template, limit);
        if (data.isEmpty() && rollback) {
            final Date rollbackDate = new Date(from.getTime() - TimeUnit.DAYS.toMillis(7));
            try {
                Date minDate = template.queryForObject("select max(createdOn) from time_and_sale"
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
                    return this.doListObjects(symbol, minDate, c.getTime(), template, limit);
                } else {
                    throw new NoDataInRangeException("No data found");
                }
            } catch (EmptyResultDataAccessException e) {
                throw new NoDataInRangeException("No data found");
            }
        }
        return data;
    }

    private List<DXTimeAndSale> doListObjects(String symbol, Date from, Date to, JdbcTemplate template, int limit) {
        final List<Object> params = new ArrayList<>(Arrays.asList(from, to));

        String query = "select t.* from time_and_sale t where t.createdOn between ? and ?";

        if (symbol != null) {
            String[] symbols = symbol.split(",");
            params.addAll(Arrays.stream(symbols).map(String::trim).filter(s -> !s.isBlank()).map(String::toUpperCase)
              .collect(Collectors.toList()));
            if (symbols.length == 1) {
                query = query + " and t.symbol = ?";
            } else {
                query = query + " and t.symbol in (" + this.getParamString(symbols) + ")";
            }
        }

        if (limit > 0)
            query += " order by t.createdOn desc limit " + limit;

        return template.query(query, params.toArray(new Object[0]), this.rowMapper);
    }

    @Override
    public void bulkInsert(List<DXTimeAndSale> in) {
        final List<Object[]> params = this.getBatchParameters(in);
        if (params.isEmpty())
            return;

        final JdbcTemplate template = JdbcTemplates.getInstance().getTemplate(false);

        final String insertOp = "insert ignore into time_and_sale (symbol, tradeIndex, createdOn, receivedOn, size, exchangeCode, "
          + "price, bidPrice, askPrice, exchangeSaleConditions, aggressorSide, spreadLeg, extendedTradingHours, validTick, type, "
          + "lateSignature, tradeThroughExempt, signaturePrint) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        template.batchUpdate(insertOp, params);

        this.logger.debug("Pushed {} records to {}", params.size(), this.getClass().getSimpleName());

        this.broadcast(in);
    }

    @Override
    public long getMaxTimestamp() {
        final JdbcTemplate template = JdbcTemplates.getInstance().getTemplate(true);
        try {
            final Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
            calendar.add(Calendar.DAY_OF_YEAR, -7);
            return Optional.ofNullable(template.queryForObject("select max(receivedOn) from time_and_sale where receivedOn >= ?",
              new Object[] { calendar.getTime()}, Date.class))
              .map(Date::getTime)
              .orElse(0L);
        } catch (EmptyResultDataAccessException ignored) {
        }
        return 0;
    }

    private List<Object[]> getBatchParameters(List<DXTimeAndSale> input) {
        final List<Object[]> params = new ArrayList<>(input.size());
        input.forEach(i ->
          params.add(new Object[] {
            i.getSymbol(),
            i.getIndex(),
            i.getCreatedOn(),
            i.getReceivedOn(),
            i.getSize(),
            i.getExchangeCode(),
            i.getRoundedPrice(),
            i.getBidPrice(),
            i.getAskPrice(),
            i.getExchangeSaleConditions(),
            i.getAggressorSide(),
            i.isSpreadLeg() ? 1 : 0,
            i.isExtendedTradingHours() ? 1 : 0,
            i.isValidTick() ? 1 : 0,
            i.getType(),
            i.isLateSignature() ? 1 : 0,
            i.getTradeThroughExempt(),
            i.getSignaturePrint()
        }));
        return params;
    }
}
