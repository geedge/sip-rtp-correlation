package com.geedgenetworks.flink.easy.application.voip.udf;

import cn.hutool.core.net.NetUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.annotation.DataTypeHint;

import java.util.ArrayList;
import java.util.List;

public class SortAddress extends IpAddressScalarFunction {

    public @DataTypeHint("STRING")
    String eval(
            String ip1, Integer port1, String ip2, Integer port2) {
        return of(Tuple2.of(ip1, port1), Tuple2.of(ip2, port2));
    }

    public static String of(
            Tuple2<String, Integer> a1, Tuple2<String, Integer> a2) {
        var list = new ArrayList<>(List.of(a1, a2));
        if (a1.f1 == null || a2.f1 == null || StringUtils.isAnyEmpty(a1.f0, a2.f0)
                || !isIpAddress(a1.f0) || !isIpAddress(a2.f0)) {
            return a1.f0 + ":" + a1.f1 + "," + a2.f0 + ":" + a2.f1;
        }
        list.sort((a, b) -> {
            if (a.f1.equals(b.f1)) {
                return compareAddress(a.f0, b.f0);
            } else {
                return a.f1.compareTo(b.f1);
            }
        });
        return String.format("%s:%s,%s:%s",
                list.get(0).f0, list.get(0).f1, list.get(1).f0, list.get(1).f1);
    }

    private static int compareAddress(String ip1, String ip2) {
        try {
            var v1 = isIpv4(ip1) ? NetUtil.ipv4ToLong(ip1) :
                    NetUtil.ipv6ToBigInteger(ip1).longValue();
            var v2 = isIpv4(ip2) ? NetUtil.ipv4ToLong(ip2) :
                    NetUtil.ipv6ToBigInteger(ip2).longValue();
            return Long.compare(v1, v2);
        } catch (Exception e) {
            return 0;
        }
    }
}