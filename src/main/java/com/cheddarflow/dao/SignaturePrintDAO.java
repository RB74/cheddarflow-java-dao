package com.cheddarflow.dao;

import com.cheddarflow.model.SignaturePrint;

import java.util.List;

public interface SignaturePrintDAO {

    List<SignaturePrint> getSignaturePrints();
    void save(SignaturePrint signaturePrint);
}
