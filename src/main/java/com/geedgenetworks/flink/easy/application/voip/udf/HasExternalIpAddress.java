package com.geedgenetworks.flink.easy.application.voip.udf;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.ScalarFunction;

public class HasExternalIpAddress extends ScalarFunction {

    private final IsExternalIpAddress isExternalIpAddress = new IsExternalIpAddress();

    public @DataTypeHint("BOOLEAN") Boolean eval(String... ipaddr) {
        if (null == ipaddr) {
            return false;
        }
        for (var ip : ipaddr) {
            return isExternalIpAddress.eval(ip);
        }
        return false;
    }
}
