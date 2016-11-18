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
require('./PrimitiveSchemaRow.less');
import {SCHEMA_TYPES, checkComplexType} from 'components/SchemaEditor/SchemaHelpers';
import SelectWithOptions from 'components/SelectWithOptions';
import AbstractSchemaRow from 'components/SchemaEditor/AbstractSchemaRow';

import {Input} from 'reactstrap';

export default class PrimitiveSchemaRow extends Component{
  constructor(props) {
    super(props);
    this.state = {
      row: props.row,
      name: props.row.name,
      type: props.row.type,
      displayType: props.row.displayType
    };
    this.onTypeChange = this.onTypeChange.bind(this);
  }
  onTypeChange(e) {
    console.log('Value', e.target.value);
    let displayType = e.target.value;
    this.setState({
      displayType: displayType,
      row: Object.assign({}, this.state.row, {displayType})
    });
  }
  render() {
    return (
      <div className="primitive-schema-row">
        <div className="field-name">
          <Input
            value={this.state.name}
          />
        </div>
        <div className="field-type">
          <SelectWithOptions
            options={SCHEMA_TYPES.types}
            onChange={this.onTypeChange}
            value={this.state.displayType}
          />
        </div>
        <div className="field-isnull text-center">
          TBD
        </div>
        {
          checkComplexType(this.state.displayType) ?
            <AbstractSchemaRow
              row={{
                displayType: this.state.displayType
              }}
            />
          :
            null
        }
      </div>
    );
  }
}

PrimitiveSchemaRow.propTypes = {
  row: PropTypes.shape({
    name: PropTypes.string,
    type: PropTypes.any,
    displayType: PropTypes.string
  })
};
