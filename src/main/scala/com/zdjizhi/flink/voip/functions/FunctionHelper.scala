package com.zdjizhi.flink.voip.functions

import org.apache.flink.api.common.functions.RichFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimerService


/**
 * A trait that provides utility functions for Flink functions.
 *
 * @author chaoc
 * @since 1.0
 */
trait FunctionHelper extends RichFunction {

  /**
   * Get the global configuration for the current Flink job.
   *
   * @return The global configuration as a Configuration object.
   */
  def getGlobalConfiguration: Configuration = getRuntimeContext.getExecutionConfig
    .getGlobalJobParameters.asInstanceOf[Configuration]

  /**
   * Get a MapState with the given name, key, and value types.
   *
   * @param name  The name of the MapState.
   * @param key   The class representing the type of keys in the MapState.
   * @param value The class representing the type of values in the MapState.
   * @tparam K The type of keys in the MapState.
   * @tparam V The type of values in the MapState.
   * @return The MapState with the given name, key, and value types.
   */
  def getMapState[K, V](name: String, key: Class[K], value: Class[V]): MapState[K, V] = {
    val descriptor = new MapStateDescriptor[K, V](name, key, value)
    getRuntimeContext.getMapState(descriptor)
  }

  /**
   * Register the next fire timestamp for the given TimerService and fire interval.
   *
   * @param timeService  The TimerService used to register the timer.
   * @param fireInterval The interval at which the timer should fire, represented as a Time object.
   */
  def registerNextFireTimestamp(timeService: TimerService, fireInterval: Time): Unit = {
    val currentTime = timeService.currentProcessingTime()
    val nextFireTime = (currentTime / fireInterval.toMilliseconds + 1) * fireInterval.toMilliseconds
    timeService.registerProcessingTimeTimer(nextFireTime)
  }

}
