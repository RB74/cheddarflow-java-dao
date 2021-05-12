package com.cheddarflow.dao;

import com.cheddarflow.jdbc.JdbcTemplates;
import com.cheddarflow.model.HolidayWindow;

import java.util.List;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

@Repository
public class HolidayWindowTable implements HolidayWindowDAO {

    private final RowMapper<HolidayWindow> rowMapper = (rs, rowNum) -> HolidayWindow.newBuilder()
      .withId(rs.getLong("id"))
      .withName(rs.getString("name"))
      .withYear(rs.getDate("year"))
      .withStartTime(rs.getTimestamp("startTime"))
      .withEndTime(rs.getTimestamp("endTime"))
      .build();

    @Override
    public List<HolidayWindow> getHolidayWindows() {
        final JdbcTemplate template = JdbcTemplates.getInstance().getTemplate(true);
        return template.query("select * from holiday where year=year(now()) and startTime > now()", this.rowMapper);
    }
}
