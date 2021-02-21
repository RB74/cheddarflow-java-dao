package com.cheddarflow.dao;

import com.cheddarflow.eventbus.GlobalEventBus;
import com.cheddarflow.jdbc.JdbcTemplates;
import com.cheddarflow.jdbc.NoDataInRangeException;
import com.cheddarflow.model.VolumeData;
import com.cheddarflow.util.DateUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Repository;

@Repository
public class VolumeTable extends AbstractDAO<VolumeData> implements VolumeDAO {

    private final RowMapper<VolumeData> volumeDataRowMapper = new VolumeDataRowMapper();

    @Autowired
    public VolumeTable(@Qualifier("normalTaskExecutor") ThreadPoolTaskExecutor taskExecutor) {
        super(taskExecutor);
    }

    @Override
    public VolumeData getVolumeData(Date d, String symbol) {
        SimpleDateFormat f = DateUtils.getDateFormat();
        String dateString = f.format(d);
        return getVolumeData(dateString, symbol);
    }

    private VolumeData getVolumeData(String dateString, String symbol) {
        final JdbcTemplate template = JdbcTemplates.getInstance().getTemplate(true);
        List<Object> params = new ArrayList<>();
        params.add(dateString);

        String query = "select * from volume where date = ?";

        if (symbol != null) {
            String[] symbols = symbol.split(",");
            params.addAll(Arrays.stream(symbols).map(String::trim).filter(s -> !s.isBlank()).map(String::toUpperCase)
              .collect(Collectors.toList()));
            if (symbols.length == 1) {
                query = query + " and symbol = ?";
            } else {
                query = query + " and symbol in (" + this.getParamString(symbols) + ")";
            }
        }

        final List<VolumeData> data = template.query(query, params.toArray(new Object[0]), this.volumeDataRowMapper);

        return !data.isEmpty() ? data.get(0) : null;
    }

    @Override
    public List<VolumeData> getVolumeData(Date from, Date to, boolean rollback) {
        SimpleDateFormat f = DateUtils.getDateFormat();
        if (from.equals(to)) {
            to = new Date(to.getTime() + TimeUnit.DAYS.toMillis(1));
        }
        String fromString = f.format(from);
        String toString = f.format(to);

        final JdbcTemplate template = JdbcTemplates.getInstance().getTemplate(true);
        final List<VolumeData> data = doGetVolumeData(fromString, toString, template);
        if (data.isEmpty() && rollback) {
            final Date rollbackDate = new Date(from.getTime() - TimeUnit.DAYS.toMillis(7));
            try {
                final Date minDate = template.queryForObject("select max(date) from volume where date >= ?",
                  new Object[] { rollbackDate }, Date.class);
                if (minDate == null)
                    throw new NoDataInRangeException("No volume data found");
                final Date maxDate = new Date(minDate.getTime() + TimeUnit.DAYS.toMillis(1));
                return doGetVolumeData(f.format(minDate), f.format(maxDate), template);
            } catch (EmptyResultDataAccessException e) {
                throw new NoDataInRangeException("No market data found");
            }
        }
        return data;
    }

    private List<VolumeData> doGetVolumeData(String fromString, String toString, JdbcTemplate template) {
        return template.query("select * from volume where date >= ? and date <= ?",
          new Object[] { fromString, toString }, this.volumeDataRowMapper);
    }

    @Override
    public void setVolumeData(VolumeData in) {
        SimpleDateFormat format = DateUtils.getDateFormat();

        try {
            final JdbcTemplate template = JdbcTemplates.getInstance().getTemplate(false);
            VolumeData existing = this.getVolumeData(in.getDate(), in.getSymbol());

            boolean differs = true;
            String insertOp;
            if (existing != null) {
                differs = !existing.similarTo(in);

                insertOp = "UPDATE volume set " +
                        "symbol = ?, " +
                          "date = ?, " +
                          "option_volume = ?, " +
                          "puts = ?, " +
                          "calls = ?, " +
                          "pct_adv = ?, " +
                          "tw_pct_adv = ?, " +
                          "adv = ?, " +
                          "option_open_int = ?, " +
                          "volume_oi_pct = ?, " +
                          "comments = ?, " +
                          "spot = ?, " +
                          "spot_chg = ?, " +
                          "bullish_pct = ?, " +
                          "neutral_pct = ?, " +
                          "bearish_pct = ?, " +
                          "put_bid_pct = ?, " +
                          "put_mid_pct = ?, " +
                          "put_ask_pct = ?, " +
                          "call_bid_pct = ?, " +
                          "call_mid_pct = ?, " +
                          "call_ask_pct = ?, " +
                          "atm_ivol = ?, " +
                          "atm_ivol_chg = ?, " +
                          "oi_puts = ?, " +
                          "oi_calls = ?, " +
                          "avg_total_puts = ?, " +
                          "avg_total_calls = ?, " +
                          "time_weight = ?, " +
                          "volume = ?, " +
                          "avg_volume = ?, " +
                          "close = ?, " +
                          "chg = ?, " +
                          "atm1 = ?, " +
                          "atm2 = ?, " +
                          "oi_puts_chg = ?, " +
                          "oi_calls_chg = ?, " +
                          "put_trades = ?, " +
                          "call_trades = ?, " +
                          "put_prem = ?, " +
                          "call_prem = ?, " +
                          "bullish_c_prem = ?, " +
                          "bearish_c_prem = ?, " +
                          "bearish_p_prem = ?, " +
                          "bullish_p_prem = ?, " +
                          "net_delta = ?, " +
                          "net_vega = ?, " +
                          "bullish_on_ask = ?, " +
                          "bearish_on_ask = ?, " +
                          "volatility20day = ?, " +
                          "volatility60day = ?, " +
                          "volatility120day = ?, " +
                          "split_adj_close = ?, " +
                          "split_adj_mult = ?, " +
                          "amex = ?, " +
                          "arca = ?, " +
                          "bxo = ?, " +
                          "bzx = ?, " +
                          "box = ?, " +
                          "cboe = ?, " +
                          "c2 = ?, " +
                          "edgx = ?, " +
                          "gem = ?, " +
                          "ise = ?, " +
                          "merc = ?, " +
                          "miax = ?, " +
                          "nom = ?, " +
                          "pearl = ?, " +
                          "phlx = ? where id = " + existing.getId();
            } else {
                insertOp = "INSERT INTO volume " +
                        "(symbol, " +
                        "date, " +
                        "option_volume, " +
                        "puts, " +
                        "calls, " +
                        "pct_adv, " +
                        "tw_pct_adv, " +
                        "adv, " +
                        "option_open_int, " +
                        "volume_oi_pct, " +
                        "comments, " +
                        "spot, " +
                        "spot_chg, " +
                        "bullish_pct, " +
                        "neutral_pct, " +
                        "bearish_pct, " +
                        "put_bid_pct, " +
                        "put_mid_pct, " +
                        "put_ask_pct, " +
                        "call_bid_pct, " +
                        "call_mid_pct, " +
                        "call_ask_pct, " +
                        "atm_ivol, " +
                        "atm_ivol_chg, " +
                        "oi_puts, " +
                        "oi_calls, " +
                        "avg_total_puts, " +
                        "avg_total_calls, " +
                        "time_weight, " +
                        "volume, " +
                        "avg_volume, " +
                        "close, " +
                        "chg, " +
                        "atm1, " +
                        "atm2, " +
                        "oi_puts_chg, " +
                        "oi_calls_chg, " +
                        "put_trades, " +
                        "call_trades, " +
                        "put_prem, " +
                        "call_prem, " +
                        "bullish_c_prem, " +
                        "bearish_c_prem, " +
                        "bearish_p_prem, " +
                        "bullish_p_prem, " +
                        "net_delta, " +
                        "net_vega, " +
                        "bullish_on_ask, " +
                        "bearish_on_ask, " +
                        "volatility20day, " +
                        "volatility60day, " +
                        "volatility120day, " +
                        "split_adj_close, " +
                        "split_adj_mult, " +
                        "amex, " +
                        "arca, " +
                        "bxo, " +
                        "bzx, " +
                        "box, " +
                        "cboe, " +
                        "c2, " +
                        "edgx, " +
                        "gem, " +
                        "ise, " +
                        "merc, " +
                        "miax, " +
                        "nom, " +
                        "pearl, " +
                        "phlx)" +
                        " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?," +
                        " ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?," +
                        " ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?," +
                        " ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            }

            template.update(insertOp, in.getSymbol(), format.format(in.getDate()), in.getOptionVolume(), in.getPuts(),
              in.getCalls(), in.getPctAdv(), in.getTwPctAdv(), in.getAdv(), in.getOptionOpenInt(), in.getVolumeOiPct(),
              in.getComments(), in.getSpot(), in.getSpotChg(), in.getBullishPct(), in.getNeutralPct(), in.getBearishPct(),
              in.getPutBidPct(), in.getPutMidPct(), in.getPutAskPct(), in.getCallBidPct(), in.getCallMidPct(), in.getCallAskPct(),
              in.getAtmIvol(), in.getAtmIvolChg(), in.getOiPuts(), in.getOiCalls(), in.getAvgTotalPuts(), in.getAvgTotalCalls(),
              in.getTimeWeight(), in.getVolume(), in.getAvgVolume(), in.getClose(), in.getChg(), in.getAtm1(), in.getAtm2(),
              in.getOiPutsChg(), in.getOiCallsChg(), in.getPutTrades(), in.getCallTrades(), in.getPutPrem(), in.getCallPrem(),
              in.getBullishCPrem(), in.getBearishCPrem(), in.getBearishPPrem(), in.getBullishPPrem(), in.getNetDelta(),
              in.getNetVega(), in.getBullishOnAsk(), in.getBearishOnAsk(), in.getVolatility20Day(), in.getVolatility60Day(),
              in.getVolatility120Day(), in.getSplitAdjClose(), in.getSplitAdjMult(), in.getAmex(), in.getArca(), in.getBxo(),
              in.getBzx(), in.getBox(), in.getCboe(), in.getC2(), in.getEdgx(), in.getGem(), in.getIse(), in.getMerc(),
              in.getMiax(), in.getNom(), in.getPearl(), in.getPhlx());

            if (differs) {
                this.broadcast(() -> getVolumeData(format.format(in.getDate()), in.getSymbol()));
            }
        } catch (Exception e) {
            this.logger.error("Could not update VolumeTable", e);
        }
    }

    private static final class VolumeDataRowMapper implements RowMapper<VolumeData> {

        @Override
        public VolumeData mapRow(ResultSet rs, int rowNum) throws SQLException {
            final VolumeData v = new VolumeData();
            v.setId(rs.getInt("id"));
            v.setSymbol(rs.getString("symbol"));
            v.setDate(rs.getDate("date"));
            v.setComments(rs.getString("comments"));
            v.setOptionVolume(this.getInteger(rs.getObject("option_volume")));
            v.setPuts(this.getInteger(rs.getObject("puts")));
            v.setCalls(this.getInteger(rs.getObject("calls")));
            v.setPctAdv(this.getDouble(rs.getObject("pct_adv")));
            v.setTwPctAdv(this.getDouble(rs.getObject("tw_pct_adv")));
            v.setAdv(this.getInteger(rs.getObject("adv")));
            v.setOptionOpenInt(this.getInteger(rs.getObject("option_open_int")));
            v.setVolumeOiPct(this.getDouble(rs.getObject("volume_oi_pct")));
            v.setSpot(this.getDouble(rs.getObject("spot")));
            v.setSpotChg(this.getDouble(rs.getObject("spot_chg")));
            v.setBullishPct(this.getDouble(rs.getObject("bullish_pct")));
            v.setNeutralPct(this.getDouble(rs.getObject("neutral_pct")));
            v.setBearishPct(this.getDouble(rs.getObject("bearish_pct")));
            v.setPutBidPct(this.getDouble(rs.getObject("put_bid_pct")));
            v.setPutMidPct(this.getDouble(rs.getObject("put_mid_pct")));
            v.setPutAskPct(this.getDouble(rs.getObject("put_ask_pct")));
            v.setCallBidPct(this.getDouble(rs.getObject("call_bid_pct")));
            v.setCallMidPct(this.getDouble(rs.getObject("call_mid_pct")));
            v.setCallAskPct(this.getDouble(rs.getObject("call_ask_pct")));
            v.setAtmIvol(this.getDouble(rs.getObject("atm_ivol")));
            v.setAtmIvolChg(this.getDouble(rs.getObject("atm_ivol_chg")));
            v.setOiPuts(this.getInteger(rs.getObject("oi_puts")));
            v.setOiCalls(this.getInteger(rs.getObject("oi_calls")));
            v.setAvgTotalPuts(this.getInteger(rs.getObject("avg_total_puts")));
            v.setAvgTotalCalls(this.getInteger(rs.getObject("avg_total_calls")));
            v.setTimeWeight(this.getDouble(rs.getObject("time_weight")));
            v.setVolume(this.getDouble(rs.getObject("volume")));
            v.setAvgVolume(this.getDouble(rs.getObject("avg_volume")));
            v.setClose(this.getDouble(rs.getObject("close")));
            v.setChg(this.getDouble(rs.getObject("chg")));
            v.setAtm1(this.getDouble(rs.getObject("atm1")));
            v.setAtm2(this.getDouble(rs.getObject("atm2")));
            v.setOiPutsChg(this.getInteger(rs.getObject("oi_puts_chg")));
            v.setOiCallsChg(this.getInteger(rs.getObject("oi_calls_chg")));
            v.setPutTrades(this.getInteger(rs.getObject("put_trades")));
            v.setCallTrades(this.getInteger(rs.getObject("call_trades")));
            v.setPutPrem(this.getDouble(rs.getObject("put_prem")));
            v.setCallPrem(this.getDouble(rs.getObject("call_prem")));
            v.setBullishCPrem(this.getDouble(rs.getObject("bullish_c_prem")));
            v.setBearishCPrem(this.getDouble(rs.getObject("bearish_c_prem")));
            v.setBearishPPrem(this.getDouble(rs.getObject("bearish_p_prem")));
            v.setBullishPPrem(this.getDouble(rs.getObject("bullish_p_prem")));
            v.setNetDelta(this.getDouble(rs.getObject("net_delta")));
            v.setNetVega(this.getDouble(rs.getObject("net_vega")));
            v.setBullishOnAsk(this.getDouble(rs.getObject("bullish_on_ask")));
            v.setBearishOnAsk(this.getDouble(rs.getObject("bearish_on_ask")));
            v.setVolatility20Day(this.getDouble(rs.getObject("volatility20day")));
            v.setVolatility60Day(this.getDouble(rs.getObject("volatility60day")));
            v.setVolatility120Day(this.getDouble(rs.getObject("volatility120day")));
            v.setSplitAdjClose(this.getDouble(rs.getObject("split_adj_close")));
            v.setSplitAdjMult(this.getDouble(rs.getObject("split_adj_mult")));
            v.setAmex(this.getInteger(rs.getObject("amex")));
            v.setArca(this.getInteger(rs.getObject("arca")));
            v.setBxo(this.getInteger(rs.getObject("bxo")));
            v.setBzx(this.getInteger(rs.getObject("bzx")));
            v.setBox(this.getInteger(rs.getObject("box")));
            v.setCboe(this.getInteger(rs.getObject("cboe")));
            v.setC2(this.getInteger(rs.getObject("c2")));
            v.setEdgx(this.getInteger(rs.getObject("edgx")));
            v.setGem(this.getInteger(rs.getObject("gem")));
            v.setIse(this.getInteger(rs.getObject("ise")));
            v.setMerc(this.getInteger(rs.getObject("merc")));
            v.setMiax(this.getInteger(rs.getObject("miax")));
            v.setNom(this.getInteger(rs.getObject("nom")));
            v.setPearl(this.getInteger(rs.getObject("pearl")));
            v.setPhlx(this.getInteger(rs.getObject("phlx")));
            return v;
        }

        int getInteger(Object o) {
            return Optional.ofNullable(o).map(Integer.class::cast).orElse(0);
        }

        double getDouble(Object o) {
            return Optional.ofNullable(o).map(Double.class::cast).orElse(0d);
        }
    }
}
