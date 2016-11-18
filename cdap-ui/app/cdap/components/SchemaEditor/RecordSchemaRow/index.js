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

import React, {PropTypes, Component} from 'react';
import {parseType} from 'components/SchemaEditor/SchemaHelpers';
import AbstractSchemaRow from 'components/SchemaEditor/AbstractSchemaRow';
require('./RecordSchemaRow.less');
import uuid from 'node-uuid';

export default class RecordSchemaRow extends Component{
  constructor(props) {
    super(props);
    if (props.row.type) {
      let rowType = parseType(props.row.type);
      let fields = rowType.type.getFields().map((field) => {
        let type = field.getType();

        let partialObj = parseType(type);

        return Object.assign({}, partialObj, {
          id: uuid.v4(),
          name: field.getName()
        });
      });
      this.state = {
        type: 'record',
        name: uuid.v4(),
        fields
      };
    } else {
      this.state = {
        type: 'record',
        name: uuid.v4(),
        fields: [
          {
            name: '',
            type: 'string',
            displayType: 'string',
            nullable: false,
            id: uuid.v4(),
            nested: false
          }
        ]
      };
    }
  }
  render() {
    return (
      <div className="record-schema-row">
        <div className="record-schema-records-row">
          {
            this.state.fields.map( (field, index) => {
              return (
                <AbstractSchemaRow
                  row={field}
                  key={index}
                />
              );
            })
          }
        </div>
      </div>
    );
  }
}

RecordSchemaRow.propTypes = {
  row: PropTypes.shape({
    name: PropTypes.string,
    type: PropTypes.any
  })
};
