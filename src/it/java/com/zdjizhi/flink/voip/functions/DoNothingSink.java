package com.zdjizhi.flink.voip.functions;

import com.zdjizhi.flink.voip.records.Record;
import com.zdjizhi.flink.voip.records.SchemaType;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;


// It dose nothing with the incoming data and simply collects metrics for the number of
// RTP and VoIP records processed per second.
public class DoNothingSink extends RichSinkFunction<ObjectNode> {

    private transient MeterView numRTPRecordsPreSecond;
    private transient MeterView numVoIPRecordsPreSecond;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        RuntimeContext runtimeContext = getRuntimeContext();
        MetricGroup metricGroup = runtimeContext.getMetricGroup();
        numRTPRecordsPreSecond = metricGroup
                .meter("numRTPRecordsPreSecond", new MeterView(1));
        numVoIPRecordsPreSecond = metricGroup
                .meter("numVoIPRecordsPreSecond", new MeterView(1));
    }

    @Override
    public void invoke(ObjectNode obj, Context context) throws Exception {
        Record record = new Record(obj);
        switch (SchemaType.of(record.getSchemaType())) {
            case RTP:
                numRTPRecordsPreSecond.markEvent();
                break;
            case VOIP:
                numVoIPRecordsPreSecond.markEvent();
                break;
            default:
        }
    }
}
