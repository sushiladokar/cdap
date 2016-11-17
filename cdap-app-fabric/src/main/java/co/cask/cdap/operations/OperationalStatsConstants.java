/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.operations;

/**
 * Constants for use in collection and retrieval of {@link OperationalStats}.
 */
public final class OperationalStatsConstants {
  public static final String JMX_DOMAIN = "co.cask.cdap.operations";
  public static final String SERVICE_NAME_KEY = "name";
  public static final String STAT_TYPE_KEY = "type";
  public static final String STAT_TYPE_INFO = "info";

  private OperationalStatsConstants() {
    // prevent instantiation.
  }
}
