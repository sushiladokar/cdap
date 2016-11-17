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

import React, {PropTypes} from 'react';
require('./PrimitiveSchemaRow.less');

export default function PrimitiveSchemaRow({row}) {
  return (
    <div className="primitive-schema-row">
      <div className="field-name">
        {row.name}
      </div>
      <div className="field-type">
        {row.displayType}
      </div>
      <div className="field-isnull">
        TBD
      </div>
    </div>
  );
}

PrimitiveSchemaRow.propTypes = {
  row: PropTypes.shape({
    name: PropTypes.string,
    type: PropTypes.any
  })
};
