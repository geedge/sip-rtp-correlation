package com.zdjizhi.flink.voip.functions

import com.zdjizhi.flink.voip.conf.FusionConfigs
import com.zdjizhi.flink.voip.data.SIPGenerator
import com.zdjizhi.flink.voip.records.{Record, StreamDirs}
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.configuration.Configuration
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.{IntNode, ObjectNode}
import org.apache.flink.streaming.api.operators.KeyedProcessOperator
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

import java.lang.{Long => JLong}
import scala.collection.JavaConverters._

class SIPPairingFunctionTest extends AnyFlatSpec with BeforeAndAfter {

  private val keySelector: KeySelector[ObjectNode, Int] = (_: ObjectNode) => 0

  private var testHarness: KeyedOneInputStreamOperatorTestHarness[Int, ObjectNode, ObjectNode] = _

  private val interval: JLong = Time.minutes(5).toMilliseconds

  before {
    val func = new SIPPairingFunction
    val operator = new KeyedProcessOperator(func)
    val tpe = TypeInformation.of(classOf[Int])
    testHarness = new KeyedOneInputStreamOperatorTestHarness(operator, keySelector, tpe)

    val config = new Configuration()
    config.set(FusionConfigs.SIP_STATE_CLEAR_INTERVAL, interval)
    testHarness.getExecutionConfig.setGlobalJobParameters(config)
    testHarness.open()
  }

  it should "properly pair SIP records" in {
    val sipGenerator = new SIPGenerator()
    val current = System.currentTimeMillis()

    val obj = sipGenerator.next
    obj.set(Record.F_COMMON_STREAM_DIR, IntNode.valueOf(StreamDirs.DOUBLE))
    val record = new StreamRecord(obj, current)

    testHarness.processElement(record)
    assert(testHarness.getOutput.asScala.toSet.contains(record))

    val nextFireTime = (testHarness.getProcessingTime / interval + 1) * interval
    assert(testHarness.getProcessingTimeService.getActiveTimerTimestamps.contains(nextFireTime))

    val obj1 = obj.deepCopy()
    obj1.set(Record.F_COMMON_STREAM_DIR, IntNode.valueOf(StreamDirs.S2C))

    val obj2 = obj.deepCopy()
    obj2.set(Record.F_COMMON_STREAM_DIR, IntNode.valueOf(StreamDirs.C2S))

    val descriptor = new MapStateDescriptor("sip-state", classOf[Address], classOf[ObjectNodeWithExpiration])

    testHarness.processElement(obj1, current)
    val mapState = testHarness.getOperator.getKeyedStateStore.getMapState(descriptor)
    assert(mapState.values().asScala.map(_.obj).toSet(obj1))

    testHarness.processElement(obj2, current)
    assert(!mapState.values().asScala.map(_.obj).toSet(obj1))
    assert(testHarness.getOutput.asScala.toSet.contains(record))
    assert(testHarness.getOutput.asScala.size == 2)
  }

}
