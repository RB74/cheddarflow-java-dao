package com.cheddarflow.dao;

import com.cheddarflow.model.VolumeData;

import java.util.Date;
import java.util.List;

public interface VolumeDAO {

    VolumeData getVolumeData(Date d, String symbol);
    List<VolumeData> getVolumeData(Date from, Date to, boolean rollback);
    void setVolumeData(VolumeData in);
}
