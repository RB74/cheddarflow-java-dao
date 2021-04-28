package com.cheddarflow.dao.dto;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import com.cheddarflow.model.TiingoIEXEvent;
import com.cheddarflow.util.MarketUtils;

import java.text.SimpleDateFormat;
import java.util.Calendar;

@JsonSerialize
public class LatestIEXData {

    private TiingoIEXEvent iexEvent;
    private float prevClose;

    public LatestIEXData() {}

    public LatestIEXData(TiingoIEXEvent iexEvent, float prevClose) {
        this.iexEvent = iexEvent;
        this.prevClose = prevClose;
    }

    public TiingoIEXEvent getIexEvent() {
        return this.iexEvent;
    }

    public void setIexEvent(TiingoIEXEvent iexEvent) {
        this.iexEvent = iexEvent;
    }

    public float getPrevClose() {
        return this.prevClose;
    }

    public void setPrevClose(float prevClose) {
        this.prevClose = prevClose;
    }

    public String getDataFrom() {
        if (this.iexEvent == null)
            return null;
        final Calendar c = Calendar.getInstance();
        c.setTime(this.iexEvent.getCreatedOn());
        final Calendar now = Calendar.getInstance();
        final boolean sameDay = c.get(Calendar.DAY_OF_YEAR) == now.get(Calendar.DAY_OF_YEAR)
          && c.get(Calendar.MONTH) == now.get(Calendar.MONTH)
          && c.get(Calendar.YEAR) == now.get(Calendar.YEAR);
        if (!sameDay || !MarketUtils.isMarketOpen()) {
            return new SimpleDateFormat("yyyy-MM-dd").format(this.iexEvent.getCreatedOn());
        }
        return null;
    }
}
