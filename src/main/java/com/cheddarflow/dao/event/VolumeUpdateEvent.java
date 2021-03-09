package com.cheddarflow.dao.event;

import com.cheddarflow.model.VolumeData;

import java.util.Collections;
import java.util.List;

public class VolumeUpdateEvent {

    private final List<VolumeData> data;

    public VolumeUpdateEvent(List<VolumeData> data) {
        this.data = data == null ? Collections.emptyList() : data;
    }

    public List<VolumeData> getData() {
        return Collections.unmodifiableList(this.data);
    }
}
