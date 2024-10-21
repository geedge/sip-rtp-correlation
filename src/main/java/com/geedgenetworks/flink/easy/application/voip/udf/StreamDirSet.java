package com.geedgenetworks.flink.easy.application.voip.udf;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.ScalarFunction;

public class StreamDirSet extends ScalarFunction {

    public @DataTypeHint("BIGINT") Long eval(Long flags) {
        if (flags == null) {
            return 8192 + 16384L;
        }
        Long r = null;
        if ((flags & 8192) == 0) {
            r += 8192;
        }
        if ((flags & 16384) == 0) {
            r += 16384;
        }
        return r;
    }

}
