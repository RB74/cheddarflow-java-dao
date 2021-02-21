package com.cheddarflow.dao;

import com.cheddarflow.jdbc.JdbcTemplates;
import com.cheddarflow.model.SectorDataInput;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class SectorTable implements SectorDAO {

    @Override
    public void setSectorData(SectorDataInput in) {
        final JdbcTemplate template = JdbcTemplates.getInstance().getTemplate(false);

        final Integer ct = template.queryForObject("select count(1) from sectors where symbol = ?", new Object[] { in.symbol },
          Integer.class);
        final boolean update = ct != null && ct > 0;
        if (update) {
            template.update("update sectors set subsector = ? where symbol = ? ", in.subsector, in.symbol);
        } else {
            //TODO: add unique key to symbol column
            template.update("INSERT INTO sectors (subsector, symbol) VALUES (?, ?) on duplicate key update subsector = ?",
              in.subsector, in.symbol, in.subsector);
        }
        template.update("update trades set subsector = ? where symbol = ?", in.subsector, in.symbol);
    }
}
