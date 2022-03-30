package com.xgsama.flink.util;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * SIterator
 *
 * @author : xgSama
 * @date : 2022/1/11 23:04:54
 */
public class SIterator<T> implements Iterator<T>, Serializable {

    public List<T> list = new ArrayList<>();

    private int cur = 0;


    @Override
    public boolean hasNext() {
        return cur < list.size();
    }

    @Override
    public T next() {
        return
                list.get(cur++);
    }

    public void add(T student) {
        list.add(student);
    }
}
