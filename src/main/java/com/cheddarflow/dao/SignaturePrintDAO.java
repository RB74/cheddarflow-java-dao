package com.cheddarflow.dao;

import com.cheddarflow.model.SignaturePrint;

import java.util.Date;
import java.util.List;
import java.util.Optional;

public interface SignaturePrintDAO {

    List<SignaturePrint> getSignaturePrints();
    Optional<SignaturePrint> findBySymbolAndDate(String symbol, Date printDate);
    void save(SignaturePrint signaturePrint);
}
