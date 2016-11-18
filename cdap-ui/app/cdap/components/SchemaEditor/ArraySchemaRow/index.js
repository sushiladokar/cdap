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
require('./ArraySchemaRow.less');

export default class ArraySchemaRow extends Component{
  constructor(props) {
    super(props);
    if (props.row.type) {
      let item = parseType(props.row.type.getItemsType());
      this.state = {
        displayType: item.displayType,
        row: props.row
      };
    } else {
      this.state = {
        row: null,
        displayType: 'string'
      };
    }
    this.onTypeChange = this.onTypeChange.bind(this);
  }
  onTypeChange(e) {
    this.setState({
      displayType: e.target.value
    });
  }
  render() {
    return (
      <div className="array-schema-row">
        <div className="array-schema-type-row">
          <SelectWithOptions
            options={SCHEMA_TYPES.types}
            value={this.state.displayType}
            onChange={this.onTypeChange}
          />
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

ArraySchemaRow.propTypes = {
  row: PropTypes.shape({
    name: PropTypes.string,
    type: PropTypes.any
  })
};
