package com.zdjizhi.flink.voip.conf

import org.apache.flink.api.java.utils.ParameterTool

import java.util.Properties
import scala.collection.JavaConverters._

/**
 * A wrapper class that extends the Flink `ParameterTool` to provide utility methods for handling
 * properties with a specific prefix. This class allows retrieving properties that start with the
 * given `prefix` and converts them into a `java.util.Properties` object.
 *
 * @param tool The original Flink `ParameterTool` instance.
 * @author chaoc
 * @since 1.0
 */
class FusionParameterTool(tool: ParameterTool) {

  /**
   * Retrieves properties from the underlying `ParameterTool` instance that start with the specified
   * `prefix`. The properties are then converted into a `java.util.Properties` object and returned.
   *
   * @param prefix The prefix to filter properties.
   * @return A `java.util.Properties` object containing the properties with the specified prefix.
   */
  def getProperties(prefix: String): Properties = {
    val map = tool.toMap.asScala.filterKeys(_.startsWith(prefix))
      .map { case (key, value) => (key.stripPrefix(prefix), value) }
    ParameterTool.fromMap(map.asJava).getProperties
  }

}
