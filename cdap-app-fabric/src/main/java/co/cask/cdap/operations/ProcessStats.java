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

import java.util.Map;

/**
 * Operational stats for services.
 */
public class ProcessStats {
  private final Map<String, Integer> services;

  public ProcessStats(Map<String, Integer> services) {
    this.services = services;
  }

  public Map<String, Integer> getServices() {
    return services;
  }
}
