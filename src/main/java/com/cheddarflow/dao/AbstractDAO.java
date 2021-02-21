package com.cheddarflow.dao;

import com.cheddarflow.eventbus.GlobalEventBus;
import com.cheddarflow.model.DXTimeAndSale;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

abstract class AbstractDAO<T> {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    protected final ThreadPoolTaskExecutor taskExecutor;

    protected AbstractDAO(ThreadPoolTaskExecutor taskExecutor) {
        this.taskExecutor = taskExecutor;
    }

    protected String getParamString(String[] strings) {
        StringBuilder builder = new StringBuilder();
        for (Iterator<String> i = Arrays.asList(strings).iterator(); i.hasNext(); ) {
            i.next();
            builder.append('?');
            if (i.hasNext())
                builder.append(',');
        }
        return builder.toString();
    }

    protected String getParamString(Collection<String> strings) {
        return getParamString(strings.toArray(new String[0]));
    }

    protected void broadcast(List<T> input) {
        if (this.taskExecutor == null) {
            throw new IllegalStateException("TaskExecutor is null");
        }
        this.taskExecutor.execute(() -> {
            for (T in : input) {
                try {
                    this.logger.debug("Broadcasting {} {}", in.getClass().getSimpleName(), in);
                    GlobalEventBus.post(in);
                } catch (Exception e) {
                    this.logger.error("Unexpected error in broadcast of {} data", in.getClass().getSimpleName(), e);
                }
            }
        });
    }

    protected void broadcast(Supplier<T> input) {
        if (this.taskExecutor == null) {
            throw new IllegalStateException("TaskExecutor is null");
        }
        this.taskExecutor.execute(() -> {
            final T in = input.get();
            if (in == null)
                return;
            try {
                this.logger.debug("Broadcasting {} {}", in.getClass().getSimpleName(), in);
                GlobalEventBus.post(in);
            } catch (Exception e) {
                this.logger.error("Unexpected error in broadcast of {} data", in.getClass().getSimpleName(), e);
            }
        });
    }
}
