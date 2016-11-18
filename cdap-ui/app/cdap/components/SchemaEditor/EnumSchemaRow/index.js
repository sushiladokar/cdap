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
import {parseType, SCHEMA_TYPES} from 'components/SchemaEditor/SchemaHelpers';
import {Input} from 'reactstrap';
import SelectWithOptions from 'components/SelectWithOptions';
require('./EnumSchemaRow.less');

export default function EnumSchemaRow({row}) {
  let rowType = parseType(row.type);
  let symbols = rowType.type.getSymbols();
  return (
    <div className="enum-schema-row">
      <div className="enum-schema-field-row">
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
      <div className="enum-schema-symbols-row">
        {
          symbols.map((symbol, index) => {
            return (
              <Input
                key={index}
                value={symbol}
              />
            );
          })
        }
      </div>
    </div>
  );
}

EnumSchemaRow.propTypes = {
  row: PropTypes.shape({
    name: PropTypes.string,
    type: PropTypes.any
  })
};
