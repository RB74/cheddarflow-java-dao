package com.cheddarflow.dao;

import com.cheddarflow.jdbc.JdbcTemplates;
import com.cheddarflow.model.SignaturePrint;

import java.util.List;

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
    public void save(SignaturePrint signaturePrint) {
        final JdbcTemplate template = JdbcTemplates.getInstance().getTemplate(false);
        template.update("update signature_print set occurrence = ?, printDate = ? where symbol = ?",
          signaturePrint.getOccurrence(), signaturePrint.getDate(), signaturePrint.getSymbol());
    }
}
