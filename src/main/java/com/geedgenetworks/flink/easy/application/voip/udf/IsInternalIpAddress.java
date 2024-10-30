package com.geedgenetworks.flink.easy.application.voip.udf;

import org.apache.flink.table.annotation.DataTypeHint;

public class IsInternalIpAddress extends IpAddressScalarFunction {

    public @DataTypeHint("BOOLEAN") Boolean eval(String ipaddr) {
        if (!isIpAddress(ipaddr)) {
            return false;
        }
        return isInternalIpAddress(ipaddr);
    }
}
