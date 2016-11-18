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
import {parseType, SCHEMA_TYPES, checkComplexType} from 'components/SchemaEditor/SchemaHelpers';
import SelectWithOptions from 'components/SelectWithOptions';
import AbstractSchemaRow from 'components/SchemaEditor/AbstractSchemaRow';

require('./UnionSchemaRow.less');

export default class UnionSchemaRow extends Component {
  constructor(props) {
    super(props);
    if (props.row.type) {
      let types = props.row.type.getTypes().map(type => parseType(type));
      this.state = {
        types
      };
    } else {
      this.state = {
        types: [
          {
            type: 'string',
            displayType: 'string',
            nullable: false,
            nested: false
          }
        ]
      };
    }
    this.onTypeChange = this.onTypeChange.bind(this);
  }
  onTypeChange(index, e) {
    let types = this.state.types;
    types[index].type = e.target.value;
    types[index].displayType = e.target.value;
    this.setState({
      types
    });
  }
  render() {
    return (
      <div className="union-schema-row">
        <div className="union-schema-types-row">
          {
            this.state.types.map((type, index) => {
              return (
                <div key={index}>
                  <SelectWithOptions
                    options={SCHEMA_TYPES.types}
                    value={type.displayType}
                    onChange={this.onTypeChange.bind(this, index)}
                  />
                  <div className="field-type"></div>
                  <div className="field-isnull text-center"> TBD </div>
                  {
                    checkComplexType(type.displayType) ?
                      <AbstractSchemaRow
                        row={{
                          displayType: type.displayType
                        }}
                      />
                    :
                      null
                  }
                </div>
              );
            })
          }
        </div>
      </div>
    );
  }
}
UnionSchemaRow.propTypes = {
  row: PropTypes.shape({
    name: PropTypes.string,
    type: PropTypes.any
  })
};
