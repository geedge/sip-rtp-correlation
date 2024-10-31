package com.geedgenetworks.flink.easy.application.voip.udf;

import org.apache.flink.table.annotation.DataTypeHint;

public class IsIpAddress extends IpAddressScalarFunction {

    public @DataTypeHint("BOOLEAN") Boolean eval(String ipaddr) {
        if (null == ipaddr) {
            return false;
        }
        return isIpAddress(ipaddr);
    }
}
