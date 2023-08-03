package com.zdjizhi.flink.voip.functions;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;

/**
 * An interface that provides utility functions for Flink functions.
 *
 * @author chaoc
 * @since 1.0
 */
public interface FunctionHelper extends RichFunction {

    /**
     * Get the global configuration for the current Flink job.
     *
     * @return The global configuration as a Configuration object.
     */

    default Configuration getGlobalConfiguration() {
        final ExecutionConfig.GlobalJobParameters globalJobParameters = getRuntimeContext()
                .getExecutionConfig().getGlobalJobParameters();
        return (Configuration) globalJobParameters;
    }

    /**
     * Get a MapState with the given name, key, and value types.
     *
     * @param name  The name of the MapState.
     * @param key   The class representing the type of keys in the MapState.
     * @param value The class representing the type of values in the MapState.
     * @param <K>   The type of keys in the MapState.
     * @param <V>   The type of values in the MapState.
     * @return The MapState with the given name, key, and value types.
     */
    default <K, V> MapState<K, V> getMapState(String name, Class<K> key, Class<V> value) {
        final MapStateDescriptor<K, V> descriptor = new MapStateDescriptor<>(name, key, value);
        return getRuntimeContext().getMapState(descriptor);
    }

    /**
     * Register the next fire timestamp for the given TimerService and fire interval.
     *
     * @param timerService The TimerService used to register the timer.
     * @param fireInterval The interval at which the timer should fire, represented as a Time object.
     */
    default void registerNextFireTimestamp(TimerService timerService, Time fireInterval) {
        long current = timerService.currentProcessingTime();
        long nextFireTime = (current / fireInterval.toMilliseconds() + 1) * fireInterval.toMilliseconds();
        timerService.registerProcessingTimeTimer(nextFireTime);
    }
}
