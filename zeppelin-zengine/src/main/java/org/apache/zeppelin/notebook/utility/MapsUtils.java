package org.apache.zeppelin.notebook.utility;

import java.util.Map;

/**
 * Collection of handy static function for maps.
 */
public class MapsUtils {
  public static boolean isNullOrEmpty(final Map<?, ?> m) {
    return m == null || m.isEmpty();
  }
}
