package com.geedgenetworks.flink.easy.application.voip.udf;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.ScalarFunction;

import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DistinctConcat extends ScalarFunction {

    public @DataTypeHint("STRING") String eval(String s1, String s2) {
        return Stream.of(s1, s2).filter(StringUtils::isNotBlank)
                .map(StringUtils::trim)
                .distinct()
                .collect(Collectors.joining(","));
    }
}
