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
import PrimitiveSchemaRow from 'components/SchemaEditor/PrimitiveSchemaRow';
import ArraySchemaRow from 'components/SchemaEditor/ArraySchemaRow';
import MapSchemaRow from 'components/SchemaEditor/MapSchemaRow';
import UnionSchemaRow from 'components/SchemaEditor/UnionSchemaRow';

require('./AbstractSchemaRow.less');

export default function AbstractSchemaRow({row}) {
  const renderSchemaRow = (row) => {
    switch(row.displayType) {
      case 'boolean':
      case 'bytes':
      case 'double':
      case 'float':
      case 'int':
      case 'long':
      case 'string':
        return (
          <PrimitiveSchemaRow row={row}/>
        );
      case 'array':
        return (
          <ArraySchemaRow row={row} />
        );
      case 'map':
        return (
          <MapSchemaRow row={row} />
        );
      case 'union':
        return (
          <UnionSchemaRow row={row} />
        );
      default:
        return null;
    }
  };
  return (
    <div className="abstract-schema-row">
      {
        renderSchemaRow(row)
      }
    </div>
  );
}
AbstractSchemaRow.propTypes = {
  row: PropTypes.shape({
    name: PropTypes.string,
    type: PropTypes.any,
    displayType: PropTypes.string
  })
};
