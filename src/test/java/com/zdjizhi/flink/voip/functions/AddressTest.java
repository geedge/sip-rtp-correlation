package com.zdjizhi.flink.voip.functions;

import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AddressTest {

    @Test
    public void testAddressOfWithDifferentPortNumbers() {
        Tuple2<String, Integer> a1 = Tuple2.of("192.168.1.10", 8080);
        Tuple2<String, Integer> a2 = Tuple2.of("192.168.1.20", 9090);

        Address address = Address.of(a1, a2);

        assertEquals("192.168.1.10", address.getIp1());
        assertEquals(8080, address.getPort1());
        assertEquals("192.168.1.20", address.getIp2());
        assertEquals(9090, address.getPort2());
    }

    @Test
    public void testAddressOfWithSamePortNumber() {
        Tuple2<String, Integer> a1 = Tuple2.of("192.168.1.10", 8080);
        Tuple2<String, Integer> a2 = Tuple2.of("192.168.1.20", 8080);

        Address address = Address.of(a1, a2);

        assertEquals("192.168.1.10", address.getIp1());
        assertEquals(8080, address.getPort1());
        assertEquals("192.168.1.20", address.getIp2());
        assertEquals(8080, address.getPort2());
    }

    @Test
    public void testAddressOfWithInvertedOrder() {
        Tuple2<String, Integer> a1 = Tuple2.of("192.168.1.20", 8080);
        Tuple2<String, Integer> a2 = Tuple2.of("192.168.1.10", 9090);

        Address address = Address.of(a1, a2);

        assertEquals("192.168.1.20", address.getIp1());
        assertEquals(8080, address.getPort1());
        assertEquals("192.168.1.10", address.getIp2());
        assertEquals(9090, address.getPort2());
    }
}
