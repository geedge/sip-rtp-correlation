package com.geedgenetworks.flink.easy.application.voip.udf;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.ScalarFunction;
import xyz.downgoon.snowflake.Snowflake;

public class SnowflakeID extends ScalarFunction {

    private static final Snowflake SNOWFLAKE = new Snowflake(1, 1);

    public @DataTypeHint("BIGINT") Long eval() {
        return SNOWFLAKE.nextId();
    }
}
