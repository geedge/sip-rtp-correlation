package com.zdjizhi.flink.voip.functions;

import com.zdjizhi.flink.voip.conf.TestConfigs;
import com.zdjizhi.flink.voip.data.RTPGenerator;
import com.zdjizhi.flink.voip.data.SIPGenerator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.util.concurrent.RateLimiter;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

// Date Generate Source.
public class DataGenSource extends RichParallelSourceFunction<ObjectNode> implements FunctionHelper {

    private transient SIPGenerator sipGenerator;
    private transient RTPGenerator rtpGenerator;
    private transient RateLimiter rateLimiter;
    private volatile boolean running;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Integer ratio = getGlobalConfiguration()
                .get(TestConfigs.VALUABLE_DATA_PROPORTION);
        this.sipGenerator = new SIPGenerator(ratio);
        this.rtpGenerator = new RTPGenerator(ratio, sipGenerator);
        final Long rate = getGlobalConfiguration()
                .get(TestConfigs.DATA_GENERATE_RATE);
        this.rateLimiter = RateLimiter.create(
                (int) (rate / getRuntimeContext().getNumberOfParallelSubtasks()));
        this.running = true;
    }

    @Override
    public void run(SourceContext<ObjectNode> ctx) throws Exception {
        while (running) {
            rateLimiter.acquire();
            ctx.collect(sipGenerator.next());
            ctx.collect(rtpGenerator.next());
        }
    }

    @Override
    public void cancel() {
        this.running = false;
    }
}
