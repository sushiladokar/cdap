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
import SelectWithOptions from 'components/SelectWithOptions';
import {parseType, SCHEMA_TYPES, checkComplexType} from 'components/SchemaEditor/SchemaHelpers';
import AbstractSchemaRow from 'components/SchemaEditor/AbstractSchemaRow';

require('./MapSchemaRow.less');

export default class MapSchemaRow extends Component {
  constructor(props) {
    super(props);
    if (props.row.type) {
      let rowType = parseType(props.row.type);
      this.state = {
        name: props.row.name,
        keysType: rowType.type.getKeysType().getTypeName(),
        valuesTypes: rowType.type.getValuesType().getTypeName()
      };
    } else {
      this.state = {
        name: props.row.name,
        keysType: 'string',
        valuesType: 'string',
        type: props.row.type || null
      };
    }
    this.onKeysTypeChange = this.onKeysTypeChange.bind(this);
    this.onValuesTypeChange = this.onValuesTypeChange.bind(this);
  }
  onKeysTypeChange(e) {
    this.setState({
      keysType: e.target.value
    });
  }
  onValuesTypeChange(e) {
    this.setState({
      valuesType: e.target.value
    });
  }
  render() {
    return (
      <div className="map-schema-row">
        <div className="map-schema-kv-row">
          <div className="key-row">
            <div className="field-name">
              <span> Key </span>
              <SelectWithOptions
                options={SCHEMA_TYPES.types}
                value={this.state.keysType}
                onChange={this.onKeysTypeChange}
              />
            </div>
            <div className="field-type"></div>
            <div className="field-isnull text-center">TBD</div>
            {
              checkComplexType(this.state.keysType) ?
                <AbstractSchemaRow
                  row={{
                    displayType: this.state.keysType
                  }}
                />
              :
                null
            }
          </div>
          <div className="value-row">
            <div className="field-name">
              <span className="text-right">Value </span>
              <SelectWithOptions
                options={SCHEMA_TYPES.types}
                value={this.state.valuesType}
                onChange={this.onValuesTypeChange}
              />
            </div>
            <div className="field-type"></div>
            <div className="field-isnull text-center">TBD</div>
              {
                checkComplexType(this.state.valuesType) ?
                  <AbstractSchemaRow
                    row={{
                      displayType: this.state.valuesType
                    }}
                  />
                :
                  null
              }
          </div>
        </div>
      </div>
    );
  }
}

MapSchemaRow.propTypes = {
  row: PropTypes.shape({
    name: PropTypes.string,
    type: PropTypes.any
  })
};
