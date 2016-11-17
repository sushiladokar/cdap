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
import SelectWithOptions from 'components/SelectWithOptions';
import {parseType, SCHEMA_TYPES} from 'components/SchemaEditor/SchemaHelpers';
require('./MapSchemaRow.less');

export default function MapSchemaRow({row}) {
  let rowType = parseType(row.type);
  let keysType = rowType.type.getKeysType().getTypeName();
  let valuesType = rowType.type.getValuesType().getTypeName();
  return (
    <div className="map-schema-row">
      <div className="map-schema-field-row">
        <div className="field-name">
          {row.name}
        </div>
        <div className="field-type">
          <SelectWithOptions
            options={SCHEMA_TYPES.types}
            value={row.displayType}
          />
        </div>
        <div className="field-isnull text-center">
          TBD
        </div>
      </div>
      <div className="map-schema-kv-row">
        <div className="key-row">
          <div className="field-name">
            <span className="text-right"> Key </span>
            <SelectWithOptions
              options={SCHEMA_TYPES.types}
              value={keysType}
            />
          </div>
          <div className="field-type"></div>
          <div className="field-isnull text-center">TBD</div>
        </div>
        <div className="value-row">
          <div className="field-name">
            <span className="text-right">Value </span>
            <SelectWithOptions
              options={SCHEMA_TYPES.types}
              value={valuesType}
            />
          </div>
          <div className="field-type"></div>
          <div className="field-isnull text-center">TBD</div>
        </div>
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
