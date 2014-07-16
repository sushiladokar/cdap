/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.data2.transaction;

/**
 * Retry strategy for failed transactions
 */
public interface RetryStrategy {
  /**
   * Returns the number of milliseconds to wait before retrying the operation.
   *
   * @param reason Reason for transaction failure.
   * @param failureCount Number of times that the request has been failed.
   * @return Number of milliseconds to wait before retrying the operation. Returning {@code 0} means
   *         retry it immediately, while negative means abort the operation.
   */
  long nextRetry(TransactionFailureException reason, int failureCount);
}
