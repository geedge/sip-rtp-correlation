package com.geedgenetworks.flink.easy.application.voip.udf;

import org.apache.flink.table.annotation.DataTypeHint;

public class HasIpAddress extends IpAddressScalarFunction {

    public @DataTypeHint("BOOLEAN") Boolean eval(String... ipaddr) {
        if (null == ipaddr) {
            return false;
        }
        for (var ip : ipaddr) {
            if (ip != null && isIpAddress(ip)) {
                return true;
            }
        }
        return false;
    }
}
