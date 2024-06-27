package com.geedgenetworks.flink.easy.application.voip.udf;

import com.zdjizhi.utils.IPUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.ScalarFunction;

import static com.zdjizhi.utils.IPUtil.isIPAddress;

public class IsInternalIpAddress extends ScalarFunction {

    public @DataTypeHint("BOOLEAN") Boolean eval(String ipaddr) {
        if (!isIPAddress(ipaddr)) {
            return false;
        }
        return IPUtil.internalIp(ipaddr);
    }
}
