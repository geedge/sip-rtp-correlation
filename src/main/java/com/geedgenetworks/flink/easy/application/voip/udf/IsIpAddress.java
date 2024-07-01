package com.geedgenetworks.flink.easy.application.voip.udf;

import com.zdjizhi.utils.IPUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.ScalarFunction;

public class IsIpAddress extends ScalarFunction {

    public @DataTypeHint("BOOLEAN") Boolean eval(String ipaddr) {
        if (null == ipaddr) {
            return false;
        }
        return IPUtil.isIPAddress(ipaddr);
    }
}
