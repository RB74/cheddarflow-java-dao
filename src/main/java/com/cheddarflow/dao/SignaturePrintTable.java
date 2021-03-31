package com.cheddarflow.dao;

import com.cheddarflow.jdbc.JdbcTemplates;
import com.cheddarflow.model.SignaturePrint;

import java.util.Date;
import java.util.List;
import java.util.Optional;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

@Repository
public class SignaturePrintTable implements SignaturePrintDAO {

    private final RowMapper<SignaturePrint> rowMapper = (rs, rowNum) -> SignaturePrint.newBuilder()
      .withSymbol(rs.getString(1))
      .withOccurrence(rs.getInt(2))
      .withDate(rs.getDate(3))
      .build();

    @Override
    public List<SignaturePrint> getSignaturePrints() {
        final JdbcTemplate template = JdbcTemplates.getInstance().getTemplate(true);
        return template.query("select symbol, occurrence, printDate from signature_print", this.rowMapper);
    }

    @Override
    public Optional<SignaturePrint> findBySymbolAndDate(String symbol, Date printDate) {
        return JdbcTemplates.getInstance().getTemplate(true)
          .query("select * from signature_print where symbol = ? and printDate = ? limit 1", this.rowMapper, symbol, printDate)
          .stream().findFirst();
    }

    @Override
    public void save(SignaturePrint signaturePrint) {
        final JdbcTemplate template = JdbcTemplates.getInstance().getTemplate(false);
        template.update("update signature_print set occurrence = ?, printDate = ? where symbol = ?",
          signaturePrint.getOccurrence(), signaturePrint.getDate(), signaturePrint.getSymbol());
    }
}
