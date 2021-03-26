package com.cheddarflow.dao.dto;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import com.cheddarflow.model.TiingoIEXEvent;

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
}
