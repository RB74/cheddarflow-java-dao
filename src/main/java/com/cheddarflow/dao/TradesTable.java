package com.cheddarflow.dao;

import com.cheddarflow.jdbc.JdbcTemplates;
import com.cheddarflow.model.MarketData;
import com.cheddarflow.model.MarketDataInput;
import com.cheddarflow.model.PutCallSummary;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
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
public class TradesTable extends AbstractDAO<MarketData> implements TradesDAO {

    private final RowMapper<MarketData> marketDataRowMapper = new MarketDataRowMapper();
    private final RowMapper<List<Integer>> summaryRowMapper = (rs, rowNum) -> {
        final List<Integer> list = new ArrayList<>(2);
        list.add(rs.getInt(1));
        list.add(rs.getInt(2));
        return list;
    };

    @Autowired
    public TradesTable(@Qualifier("normalTaskExecutor") ThreadPoolTaskExecutor taskExecutor) {
        super(taskExecutor);
    }

    @Override
    public PutCallSummary getPutCallSummary(Date from, Date to, String symbolSearch) {
        if (from.equals(to)) {
            to = new Date(to.getTime() + TimeUnit.DAYS.toMillis(1));
        }

        final PutCallSummary summary = new PutCallSummary();

        final JdbcTemplate template = JdbcTemplates.getInstance().getTemplate(true);

        final List<Object> params = new ArrayList<>();
        params.add(from);
        params.add(to);

        final String symbol;
        if (symbolSearch != null) {
            String[] symbols = symbolSearch.split(",");
            params.addAll(Arrays.stream(symbols).map(String::trim).filter(s -> !s.isBlank()).collect(Collectors.toList()));
            params.add(from);
            params.add(to);
            params.addAll(Arrays.asList(symbols));
            if (symbols.length == 1) {
                symbol = " and symbol = ?";
            } else {
                symbol = " and symbol in (" + this.getParamString(symbols) + ")";
            }
        } else {
            params.add(from);
            params.add(to);
            symbol = "";
        }

        List<Integer> sizes = template.queryForObject(
          "select (select sum(size) from trades where timestamp between ? and ? "
            + " and PC = 'P' and side > 0 " + symbol + ") as puts, (select sum(size) from trades where timestamp between ? and ? "
            + " and PC = 'C' and side > 0 " + symbol + ") as calls from dual", params.toArray(new Object[0]),
          this.summaryRowMapper);

        if (sizes != null) {
            summary.setPutCount(sizes.get(0));
            summary.setCallCount(sizes.get(1));
        }

        return summary;
    }

    @Override
    public MarketData getMarketData(Date date, int tradeid) {

        final JdbcTemplate template = JdbcTemplates.getInstance().getTemplate(true);
        final List<MarketData> data = template.query("select t.* from trades t where t.timestamp = ? AND t.tradeid = ?",
          new Object[] { date, tradeid}, this.marketDataRowMapper);

        return !data.isEmpty() ? data.get(0) : null;
    }

    @Override
    public List<MarketData> getMarketData(Date from, Date to, String symbol, int limit) {
        if (from.equals(to)) {
            to = new Date(to.getTime() + TimeUnit.DAYS.toMillis(1));
        }

        final JdbcTemplate template = JdbcTemplates.getInstance().getTemplate(true);

        final List<Object> params = new ArrayList<>(Arrays.asList(from, to, 0));

        String query = "select t.* from trades t where t.timestamp between ? and ? and t.side > ?";

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
            query += " order by t.timestamp desc limit " + limit;

        return template.query(query, params.toArray(new Object[0]), this.marketDataRowMapper);
    }

    @Override
    public boolean setMarketData(MarketDataInput in) {
        if (in.side <= -2) return false;

        final JdbcTemplate template = JdbcTemplates.getInstance().getTemplate(false);
        final List<Integer> existing = template.query("select id from trades where timestamp = ? and tradeid = ?",
          new Object[] { in.timestamp, in.tradeid }, (rs, i) -> rs.getInt(1));
        if (!existing.isEmpty())
            return false;

        final List<String> subsectors = template.queryForList("select subsector from sectors where symbol = ?",
          new Object[] { in.symbol }, String.class);
        final String subsector = subsectors.isEmpty() ? "" : subsectors.get(0);

        String insertOp = "INSERT INTO trades ("
          + "tradeid, size, symbol, expiry, strike, type, price, side, exch, volume, "
          + "cond, ivol, ivolchg, ivolchgpct, delta, deltadollar, spot, spotchg, vega, vegadollar, theta, "
          + "bidprice, bidsize, askprice, asksize, notional, oi, sentiment, pc, thirdfriday, otm, events, section, subsector, "
          + "timestamp, date, time, unusual, highlyunusual) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,"
          + " ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        template.update(insertOp, in.tradeid, in.size, in.symbol, in.expiry, in.strike, in.type, in.getRoundedPrice(), in.side,
          in.exch, in.volume, in.condition, in.ivol, in.ivolchg, in.ivolchgpct, in.delta, in.deltadollar,
          in.spot, in.spotchg, in.vega, in.vegadollar, in.theta, in.bidprice, in.bidsize, in.askprice, in.asksize, in.notional,
          in.oi, in.sentiment, in.pc, in.thirdfriday ? 1 : 0, in.otm ? 1 : 0, in.events, in.section, subsector, in.timestamp,
          in.timestamp, formatTimeInStupidOldFormat(in.timestamp), in.isUnusual(), in.isHighlyUnusual());

        this.broadcast(() -> {
            try {
                return getMarketData(in, subsector);
            } catch (ParseException e) {
                logger.error("Error broadcasting {}", in, e);
            }
            return in;
        });
        return true;
    }

    private MarketData getMarketData(MarketDataInput in, String subsector) throws ParseException {
        final MarketData marketData = new MarketData();
        marketData.setSentiment(in.sentiment);
        marketData.setSize(in.size);
        marketData.setSymbol(in.symbol);
        marketData.setExpiry(new SimpleDateFormat("yyyy-MM-dd").parse(in.expiry));
        marketData.setStrike(in.strike);
        marketData.setType(in.type);
        marketData.setPrice(in.price);
        marketData.setOptionType(in.pc);
        marketData.setSide(in.side);
        marketData.setExch(in.exch);
        marketData.setTimestamp(in.timestamp);
        marketData.setVolume(in.volume);
        marketData.setCondition(in.condition);
        marketData.setThirdFriday(in.thirdfriday);
        marketData.setIvol(in.ivol);
        marketData.setIvolChg(in.ivolchg);
        marketData.setIvolChgPct(in.ivolchgpct);
        marketData.setDelta(in.delta);
        marketData.setDeltaDollar(in.deltadollar);
        marketData.setSpot(in.spot);
        marketData.setSpotChg(in.spotchg);
        marketData.setVega(in.vega);
        marketData.setVegaDollar(in.vegadollar);
        marketData.setTheta(in.theta);
        marketData.setEvents(in.events);
        marketData.setBidPrice(in.bidprice);
        marketData.setBidSize(in.bidsize);
        marketData.setAskPrice(in.askprice);
        marketData.setAskSize(in.asksize);
        marketData.setNotional(in.notional);
        marketData.setOi(in.oi);
        marketData.setOutOfMoney(in.otm);
        marketData.setSubsector(subsector);
        marketData.setSection(in.section);
        marketData.setSector("");
        marketData.setUnusual(in.isUnusual());
        marketData.setHighlyUnusual(in.isHighlyUnusual());
        return marketData;
    }

    private double formatTimeInStupidOldFormat(Date date) {
        Calendar c = Calendar.getInstance(TimeZone.getTimeZone("America/New_York"));
        c.setTime(date);
        return c.get(Calendar.HOUR_OF_DAY) * 60 * 60 + c.get(Calendar.MINUTE) * 60 +
          c.get(Calendar.SECOND) + c.get(Calendar.MILLISECOND) / 1000d;
    }

    @Override
    public long getMaxTimestamp() {
        final JdbcTemplate template = JdbcTemplates.getInstance().getTemplate(true);
        try {
            final Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
            calendar.add(Calendar.DAY_OF_YEAR, -7);
            return Optional.ofNullable(template.queryForObject("select max(timestamp) from trades where timestamp >= ?",
              new Object[] { calendar.getTime()}, Date.class))
              .map(Date::getTime)
              .orElse(0L);
        } catch (EmptyResultDataAccessException ignored) {
        }
        return 0;
    }

    private static final class MarketDataRowMapper implements RowMapper<MarketData> {

        @Override
        public MarketData mapRow(ResultSet rs, int rowNum) throws SQLException {
            final MarketData marketData = new MarketData();
            marketData.setId(rs.getInt("id"));
            marketData.setSentiment(Optional.ofNullable(rs.getString("sentiment")).orElse(""));
            marketData.setSize(rs.getInt("size"));
            marketData.setSymbol(rs.getString("symbol"));
            marketData.setExpiry(rs.getDate("expiry"));
            marketData.setStrike(rs.getFloat("strike"));
            marketData.setType(Optional.ofNullable(rs.getString("type")).orElse(""));
            marketData.setPrice(rs.getFloat("price"));
            marketData.setOptionType(rs.getString("pc"));
            marketData.setSide(rs.getInt("side"));
            marketData.setExch(rs.getString("exch"));
            marketData.setTimestamp(rs.getTimestamp("timestamp"));
            marketData.setVolume(rs.getInt("volume"));
            marketData.setCondition(Optional.ofNullable(rs.getString("cond")).orElse(""));
            marketData.setThirdFriday(Optional.ofNullable(rs.getObject("thirdfriday"))
              .map(Boolean.class::cast).orElse(false));
            marketData.setIvol(rs.getDouble("ivol"));
            marketData.setIvolChg(rs.getDouble("ivolchg"));
            marketData.setIvolChgPct(Optional.ofNullable(rs.getObject("ivolchgpct")).map(Double.class::cast)
              .orElse(0.0d));
            marketData.setDelta(rs.getDouble("delta"));
            marketData.setDeltaDollar(rs.getDouble("deltadollar"));
            marketData.setSpot(rs.getDouble("spot"));
            marketData.setSpotChg(rs.getDouble("spotchg"));
            marketData.setVega(rs.getDouble("vega"));
            marketData.setVegaDollar(rs.getDouble("vegadollar"));
            marketData.setTheta(rs.getDouble("theta"));
            marketData.setEvents(rs.getString("events"));
            marketData.setBidPrice(rs.getDouble("bidprice"));
            marketData.setBidSize(rs.getDouble("bidsize"));
            marketData.setAskPrice(rs.getDouble("askprice"));
            marketData.setAskSize(rs.getDouble("asksize"));
            marketData.setNotional(rs.getDouble("notional"));
            marketData.setOi(rs.getDouble("oi"));
            marketData.setOutOfMoney(Optional.ofNullable(rs.getObject("otm")).map(Boolean.class::cast)
              .orElse(false));
            marketData.setSubsector(Optional.ofNullable(rs.getString("subsector")).orElse(""));
            marketData.setSection(Optional.ofNullable(rs.getString("section")).orElse(""));
            marketData.setSector("");
            marketData.setUnusual(Optional.ofNullable(rs.getObject("unusual")).map(Boolean.class::cast)
              .orElse(false));
            marketData.setHighlyUnusual(Optional.ofNullable(rs.getObject("highlyunusual")).map(Boolean.class::cast)
              .orElse(false));
            return marketData;
        }
    }
}
