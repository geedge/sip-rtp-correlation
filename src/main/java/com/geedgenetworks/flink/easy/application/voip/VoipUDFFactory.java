package com.geedgenetworks.flink.easy.application.voip;

import com.geedgenetworks.flink.easy.application.voip.udf.IsInternalIPAddress;
import com.geedgenetworks.flink.easy.application.voip.udf.IsIpAddress;
import com.geedgenetworks.flink.easy.application.voip.udf.StreamDir;
import com.geedgenetworks.flink.easy.common.api.UDFFactory;
import org.apache.flink.table.functions.UserDefinedFunction;

import java.util.HashMap;
import java.util.Map;

public class VoipUDFFactory implements UDFFactory {

    private static final Map<String, UserDefinedFunction> R =
            new HashMap<>() {{
                put("IS_IP_ADDRESS", new IsIpAddress());
                put("IS_INTERNAL_IP_ADDRESS", new IsInternalIPAddress());
                put("STREAM_DIR", new StreamDir());
            }};

    @Override
    public Map<String, UserDefinedFunction> register() {
        return R;
    }
}
