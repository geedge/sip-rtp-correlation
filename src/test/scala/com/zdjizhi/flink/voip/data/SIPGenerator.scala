package com.zdjizhi.flink.voip.data

import com.zdjizhi.flink.voip.records.{Record, SIPRecord, StreamDirs}
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.{IntNode, ObjectNode, TextNode}

// 生成 SIP 数据行
class SIPGenerator(ratio: Int = 40) extends Generator[ObjectNode](ratio) {

  private val mapper = new ObjectMapper()

  override protected def generate: ObjectNode = {
    val obj = mapper.createObjectNode()

    obj.set(SIPRecord.F_CALL_ID, TextNode.valueOf(Generator.nextID))

    val dir = if (Generator.random.nextBoolean()) StreamDirs.S2C else StreamDirs.C2S
    obj.set(Record.F_COMMON_STREAM_DIR, IntNode.valueOf(dir))

    obj.set(Record.F_COMMON_SERVER_IP, TextNode.valueOf(Generator.nextIP))
    obj.set(Record.F_COMMON_SERVER_PORT, IntNode.valueOf(Generator.nextPort))
    obj.set(Record.F_COMMON_CLIENT_IP, TextNode.valueOf(Generator.nextIP))
    obj.set(Record.F_COMMON_CLIENT_PORT, IntNode.valueOf(Generator.nextPort))

    obj.set(SIPRecord.F_ORIGINATOR_SDP_CONNECT_IP, TextNode.valueOf(Generator.nextIP))
    obj.set(SIPRecord.F_ORIGINATOR_SDP_MEDIA_PORT, IntNode.valueOf(Generator.nextPort))
    obj.set(SIPRecord.F_RESPONDER_SDP_CONNECT_IP, TextNode.valueOf(Generator.nextIP))
    obj.set(SIPRecord.F_RESPONDER_SDP_MEDIA_PORT, IntNode.valueOf(Generator.nextPort))

    obj
  }

  override def afterState(v: ObjectNode): ObjectNode = {
    import Record.Implicits._
    v.streamDir match {
      case StreamDirs.C2S =>
        v.set(Record.F_COMMON_STREAM_DIR, IntNode.valueOf(StreamDirs.S2C))
      case StreamDirs.S2C =>
        v.set(Record.F_COMMON_STREAM_DIR, IntNode.valueOf(StreamDirs.C2S))
      case _ =>
    }
    v
  }
}
