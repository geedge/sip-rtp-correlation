package com.zdjizhi.flink.voip.functions;

import com.google.common.collect.Lists;
import com.zdjizhi.utils.IPUtil;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.List;


/**
 * A pojo class representing an address with two IP and port pairs.
 *
 * @author chaoc
 * @since 1.0
 */
@Data
@AllArgsConstructor
class Address {

    // The first IP address.
    private final String ip1;

    //The first port number.
    private final int port1;

    //The second IP address.
    private final String ip2;

    //The second port number.
    private final int port2;

    /**
     * Creates an Address instance based on two tuples containing (String, Int) representing address information.
     * The method sorts the addresses based on the port number, and if the ports are equal, it sorts them based on
     * the numeric value of the IP address.
     *
     * @param a1 The first address information as a tuple (IP address, port).
     * @param a2 The second address information as a tuple (IP address, port).
     * @return An Address instance with addresses sorted and reordered.
     */
    public static Address of(Tuple2<String, Integer> a1, Tuple2<String, Integer> a2) {
        List<Tuple2<String, Integer>> list = Lists.newArrayList(a1, a2);
        list.sort((a, b) -> {
            if (a.f1.equals(b.f1)) {
                return Long.compare(IPUtil.getIpDesimal(a.f0), IPUtil.getIpDesimal(b.f0));
            } else {
                return a.f1.compareTo(b.f1);
            }
        });
        return new Address(list.get(0).f0, list.get(0).f1, list.get(1).f0, list.get(1).f1);
    }
}
