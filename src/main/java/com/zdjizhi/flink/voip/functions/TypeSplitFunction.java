package com.zdjizhi.flink.voip.functions;

import com.zdjizhi.flink.voip.records.Record;
import com.zdjizhi.flink.voip.records.SchemaType;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * A ProcessFunction that splits ObjectNode records based on their 'schemaType' field.
 * It outputs SIP records to the 'sipSchemaTypeOutputTag' and RTP records to the 'rtpSchemaTypeOutputTag'.
 *
 * @author chaoc
 * @since 1.0
 */
public class TypeSplitFunction extends ProcessFunction<ObjectNode, ObjectNode> {

    /**
     * OutputTag for SIP records.
     */
    public static final OutputTag<ObjectNode> SIP_OUTPUT_TAG =
            new OutputTag<>("schema-type-sip", TypeInformation.of(ObjectNode.class));
    /**
     * OutputTag for RTP records.
     */
    public static final OutputTag<ObjectNode> RTP_OUTPUT_TAG =
            new OutputTag<>("schema-type-rtp", TypeInformation.of(ObjectNode.class));

    @Override
    public void processElement(ObjectNode obj,
                               ProcessFunction<ObjectNode, ObjectNode>.Context ctx,
                               Collector<ObjectNode> out) throws Exception {
        final Record record = new Record(obj);
        switch (SchemaType.of(record.getSchemaType())) {
            case RTP:
                ctx.output(RTP_OUTPUT_TAG, obj);
                break;
            case SIP:
                ctx.output(SIP_OUTPUT_TAG, obj);
                break;
            default:
        }
    }
}
