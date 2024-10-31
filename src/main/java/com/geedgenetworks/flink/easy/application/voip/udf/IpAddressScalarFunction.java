package com.geedgenetworks.flink.easy.application.voip.udf;

import cn.hutool.core.lang.Validator;
import cn.hutool.core.net.NetUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.functions.ScalarFunction;

public abstract class IpAddressScalarFunction extends ScalarFunction {

    protected static boolean isIpAddress(String ipaddr) {
        return isIpv4(ipaddr) || isIpv6(ipaddr);
    }

    protected static boolean isIpv4(String ipaddr) {
        return Validator.isIpv4(StringUtils.trim(ipaddr));
    }

    protected static boolean isIpv6(String ipaddr) {
        return Validator.isIpv6(StringUtils.trim(ipaddr));
    }

    protected static boolean isInternalIpAddress(String ipaddr) {
        return NetUtil.isInnerIP(StringUtils.trim(ipaddr));
    }
}
