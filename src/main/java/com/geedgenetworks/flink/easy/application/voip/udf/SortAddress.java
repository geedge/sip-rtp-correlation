package com.geedgenetworks.flink.easy.application.voip.udf;

import com.google.common.collect.Lists;
import com.zdjizhi.utils.IPUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.ScalarFunction;

public class SortAddress extends ScalarFunction {

    public @DataTypeHint("STRING") String eval(String ip1, Integer port1, String ip2, Integer port2) {
        return of(Tuple2.of(ip1, port1), Tuple2.of(ip2, port2));
    }

    public static String of(Tuple2<String, Integer> a1, Tuple2<String, Integer> a2) {
        var list = Lists.newArrayList(a1, a2);
        list.sort((a, b) -> {
            if (a.f1.equals(b.f1)) {
                return Long.compare(IPUtil.getIpDesimal(a.f0), IPUtil.getIpDesimal(b.f0));
            } else {
                return a.f1.compareTo(b.f1);
            }
        });
        return list.get(0).f0 + list.get(0).f1 + list.get(1).f0 + list.get(1).f1;
    }
}