package com.sl.transport.common.util;

public interface PageFunction<X,T> {
    void map(X x, T t);
}
