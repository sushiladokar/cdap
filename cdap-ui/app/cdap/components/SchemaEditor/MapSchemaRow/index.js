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
// import AbstractSchemaRow from 'components/SchemaEditor/AbstractSchemaRow';
import {parseType} from 'components/SchemaEditor/SchemaHelpers';

export default function MapSchemaRow({row}) {
  let rowType = parseType(row.type);
  let keysType = rowType.type.getKeysType().getTypeName();
  let valuesType = rowType.type.getValuesType().getTypeName();
  return (
    <div className="map-schema-row">
      <div className="key-row">
        <span> Key </span>
        <span>{keysType}</span>
      </div>
      <div className="value-row">
        <span>Value </span>
        <span>{valuesType}</span>
      </div>
    </div>
  );
}

MapSchemaRow.propTypes = {
  row: PropTypes.shape({
    name: PropTypes.string,
    type: PropTypes.any
  })
};
