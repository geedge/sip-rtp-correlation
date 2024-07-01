package com.geedgenetworks.flink.easy.application.voip.udf;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.ScalarFunction;

public class FindNotBlank extends ScalarFunction {

    public @DataTypeHint("STRING") String eval(String s1, String s2) {
        if (StringUtils.isBlank(s1) && StringUtils.isNotBlank(s2)) {
            return s2;
        }
        return s1;
    }
}
