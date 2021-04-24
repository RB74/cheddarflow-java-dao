package com.cheddarflow.dao;

import com.cheddarflow.dao.dto.LatestIEXData;
import com.cheddarflow.jdbc.JdbcTemplates;
import com.cheddarflow.jdbc.NoDataInRangeException;
import com.cheddarflow.model.TiingoEventType;
import com.cheddarflow.model.TiingoIEXEvent;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.checkerframework.checker.units.qual.s;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

@Repository
public class TiingoIEXEventTable extends AbstractDAO<TiingoIEXEvent> implements TiingoIEXEventDAO {

    private final RowMapper<TiingoIEXEvent> rowMapper = (rs, i) -> TiingoIEXEvent.newBuilder()
      .withId(rs.getLong("id"))
      .withSymbol(rs.getString("symbol"))
      .withTiingoEventType(TiingoEventType.valueOf(rs.getString("tiingoEventType")))
      .withCreatedOn(rs.getTimestamp("createdOn"))
      .withBidSize(rs.getInt("bidSize"))
      .withBidPrice(rs.getFloat("bidPrice"))
      .withMidPrice(rs.getFloat("midPrice"))
      .withAskPrice(rs.getFloat("askPrice"))
      .withAskSize(rs.getInt("askSize"))
      .withLastPrice(rs.getFloat("lastPrice"))
      .withLastSize(rs.getInt("lastSize"))
      .withHalted(rs.getInt("halted") == 1)
      .withAfterHours(rs.getInt("afterHours") == 1)
      .withIntermarketSweepOrder(rs.getInt("intermarketSweepOrder") == 1)
      .withOddLot(rs.getInt("oddLot") == 1)
      .withSubjectToNMSRule611(rs.getInt("subjectToNMSRule611") == 1)
      .build();

    private final RowMapper<LatestIEXData> latestRowMapper = (rs, i) -> {
        final TiingoIEXEvent event = TiingoIEXEvent.newBuilder()
          .withId(rs.getLong("id"))
          .withSymbol(rs.getString("symbol"))
          .withTiingoEventType(TiingoEventType.valueOf(rs.getString("tiingoEventType")))
          .withCreatedOn(rs.getTimestamp("createdOn"))
          .withBidSize(rs.getInt("bidSize"))
          .withBidPrice(rs.getFloat("bidPrice"))
          .withMidPrice(rs.getFloat("midPrice"))
          .withAskPrice(rs.getFloat("askPrice"))
          .withAskSize(rs.getInt("askSize"))
          .withLastPrice(rs.getFloat("lastPrice"))
          .withLastSize(rs.getInt("lastSize"))
          .withHalted(rs.getInt("halted") == 1)
          .withAfterHours(rs.getInt("afterHours") == 1)
          .withIntermarketSweepOrder(rs.getInt("intermarketSweepOrder") == 1)
          .withOddLot(rs.getInt("oddLot") == 1)
          .withSubjectToNMSRule611(rs.getInt("subjectToNMSRule611") == 1)
          .withHash(rs.getInt("hash"))
          .build();
        final float lastPrice = rs.getFloat("prevClose");
        return new LatestIEXData(event, lastPrice);
    };

    public TiingoIEXEventTable() {
        super(null);
    }

    @Override
    public void bulkInsert(List<TiingoIEXEvent> in) {
        final List<Object[]> params = this.getBatchParameters(in);
        if (params.isEmpty())
            return;

        final JdbcTemplate template = JdbcTemplates.getInstance().getTemplate(false);
        final String insertOp = "insert ignore into tiingo_iex_data (symbol, tiingoEventType, createdOn, bidSize, bidPrice, "
          + "midPrice, askPrice, askSize, lastPrice, lastSize, halted, afterHours, intermarketSweepOrder, oddLot, "
          + "subjectToNMSRule611, hash) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        template.batchUpdate(insertOp, params);

        this.logger.trace("Pushed {} records to {}", params.size(), this.getClass().getSimpleName());
    }

    private List<Object[]> getBatchParameters(List<TiingoIEXEvent> input) {
        final List<Object[]> params = new ArrayList<>(input.size());
        input.forEach(i ->
          params.add(new Object[] {
            i.getSymbol(),
            i.getTiingoEventType().name(),
            i.getCreatedOn(),
            i.getBidSize(),
            i.getBidPrice(),
            i.getMidPrice(),
            i.getAskPrice(),
            i.getAskSize(),
            i.getLastPrice(),
            i.getLastSize(),
            i.isHalted() ? 1 : 0,
            i.isAfterHours() ? 1 : 0,
            i.isIntermarketSweepOrder() ? 1 : 0,
            i.isOddLot() ? 1 : 0,
            i.isSubjectToNMSRule611() ? 1 : 0,
            i.getHash()
          })
        );
        return params;
    }

    @Override
    public List<TiingoIEXEvent> listObjects(Date from, Date to, List<String> symbolList) {
        if (from.equals(to)) {
            to = new Date(to.getTime() + TimeUnit.DAYS.toMillis(1));
        }

        final JdbcTemplate template = JdbcTemplates.getInstance().getTemplate(true);
        final List<Object> params = new ArrayList<>(Arrays.asList(from, to));

        String query = "select t.* from tiingo_iex_data t where t.createdOn between ? and ?";

        if (symbolList != null && !symbolList.isEmpty()) {
            params.addAll(symbolList.stream().filter(s -> !s.isBlank()).map(String::toUpperCase).collect(Collectors.toList()));
            if (symbolList.size() == 1) {
                query = query + " and t.symbol = ?";
            } else {
                query = query + " and t.symbol in (" + this.getParamString(symbolList) + ")";
            }
        }

        query += " order by t.createdOn desc";

        return template.query(query, this.rowMapper, params.toArray(new Object[0]));
    }

    @Override
    public List<TiingoIEXEvent> mostRecentObjects(List<String> symbols) {
        if (symbols == null || symbols.isEmpty())
            return Collections.emptyList();

        final JdbcTemplate template = JdbcTemplates.getInstance().getTemplate(true);
        final StringBuilder builder =
          new StringBuilder("select * from (select * from tiingo_iex_data where symbol = ? order by createdOn desc limit 1) b ");
        final int size = symbols.size();
        if (size > 1) {
            builder.append(
              " union (select * from tiingo_iex_data where symbol = ? order by createdOn desc limit 1) ".repeat(size - 1));
        }
        return template.query(builder.toString(), this.rowMapper, symbols.toArray(new Object[0]));
    }

    @Override
    public LatestIEXData findLatestObject(String symbol) {
        final JdbcTemplate template = JdbcTemplates.getInstance().getTemplate(true);

        final Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        final Date end = calendar.getTime();
        calendar.add(Calendar.DAY_OF_YEAR, -1);
        final Date start = calendar.getTime();

        final String sql = "select t.*, (select b.lastPrice FROM tiingo_iex_data b WHERE b.symbol = ? AND b.createdOn between "
          + "? and ? and b.tiingoEventType = ? ORDER BY b.createdOn DESC LIMIT 1) as prevClose from (SELECT a.* "
          + "FROM tiingo_iex_data a WHERE a.symbol = ? and a.tiingoEventType = ? ORDER BY a.createdOn DESC LIMIT 1) t";
        return template.queryForObject(sql, this.latestRowMapper, symbol, start, end, TiingoEventType.LAST_TRADE.name(), symbol,
          TiingoEventType.LAST_TRADE.name());
    }

    @Override
    public List<TiingoIEXEvent> listObjects(Date from, Date to, String symbol, boolean rollback, int limit) {
        if (from.equals(to)) {
            to = new Date(to.getTime() + TimeUnit.DAYS.toMillis(1));
        }

        final JdbcTemplate template = JdbcTemplates.getInstance().getTemplate(true);
        final List<TiingoIEXEvent> data = this.doListObjects(symbol, from, to, template, limit);
        if (data.isEmpty() && rollback) {
            final Date rollbackDate = new Date(from.getTime() - TimeUnit.DAYS.toMillis(7));
            try {
                Date minDate = template.queryForObject("select max(createdOn) from tiingo_iex_data"
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

    private List<TiingoIEXEvent> doListObjects(String symbol, Date from, Date to, JdbcTemplate template, int limit) {
        final List<Object> params = new ArrayList<>(Arrays.asList(from, to));

        String query = "select t.* from tiingo_iex_data t where t.createdOn between ? and ?";

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

        return template.query(query, this.rowMapper, params.toArray(new Object[0]));
    }

    @Override
    public long getMaxTimestamp() {
        final JdbcTemplate template = JdbcTemplates.getInstance().getTemplate(true);
        try {
            final Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
            calendar.add(Calendar.DAY_OF_YEAR, -7);
            return Optional.ofNullable(template.queryForObject("select max(receivedOn) from tiingo_iex_data where createdOn >= ?",
              Date.class, calendar.getTime()))
              .map(Date::getTime)
              .orElse(0L);
        } catch (EmptyResultDataAccessException ignored) {
        }
        return 0;
    }
}
