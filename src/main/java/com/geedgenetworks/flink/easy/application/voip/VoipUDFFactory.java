package com.geedgenetworks.flink.easy.application.voip;

import com.geedgenetworks.flink.easy.application.voip.udf.*;
import com.geedgenetworks.flink.easy.common.api.UDFFactory;
import org.apache.flink.table.functions.UserDefinedFunction;

import java.util.HashMap;
import java.util.Map;

public class VoipUDFFactory implements UDFFactory {

    private static final Map<String, UserDefinedFunction> R =
            new HashMap<>() {{
                put("IS_IP_ADDRESS", new IsIpAddress());

                put("IS_INTERNAL_IP_ADDRESS", new IsInternalIpAddress());
                put("IS_EXTERNAL_IP_ADDRESS", new IsExternalIpAddress());

                put("HAS_IP_ADDRESS", new HasIpAddress());
                put("HAS_EXTERNAL_IP_ADDRESS", new HasExternalIpAddress());

                put("STREAM_DIR", new StreamDir());
                put("STREAM_DIR_SET", new StreamDirSet());
                put("FIND_NOT_BLANK", new FindNotBlank());
                put("DISTINCT_CONCAT", new DistinctConcat());
                put("SORT_ADDRESS", new SortAddress());

                put("SNOWFLAKE_ID", new SnowflakeID());
            }};

    @Override
    public Map<String, UserDefinedFunction> register() {
        return R;
    }
}
