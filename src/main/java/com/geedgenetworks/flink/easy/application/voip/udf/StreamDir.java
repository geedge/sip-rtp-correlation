package com.geedgenetworks.flink.easy.application.voip.udf;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.ScalarFunction;

public class StreamDir extends ScalarFunction {

    public @DataTypeHint("INT") Integer eval(Long flags) {
        int v = 0;
        if (flags == null) {
            return v;
        }
        if ((flags & 8192) == 8192) {
            v += 1;
        }
        if ((flags & 16384) == 16384) {
            v += 2;
        }
        return v;
    }

    public static void main(String[] args) {
        System.out.println(8192L + 16384L);
        System.out.println(new StreamDir().eval(8192L + 16384L));
    }
}