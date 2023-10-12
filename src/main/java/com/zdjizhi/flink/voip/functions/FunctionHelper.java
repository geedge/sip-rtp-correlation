package com.zdjizhi.flink.voip.functions;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RichFunction;
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

    default void registerNextFireTimestamp(TimerService timerService, long interval) {
        long current = timerService.currentWatermark();
        timerService.registerEventTimeTimer(current + interval);
    }
}
