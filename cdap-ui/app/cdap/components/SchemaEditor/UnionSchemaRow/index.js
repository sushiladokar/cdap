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
    if (typeof props.row.type === 'object') {
      let types = props.row.type.getTypes();
      let parsedTypes = types.map(type => parseType(type));
      let displayTypes = parsedTypes.map(type => type.displayType);

      this.state = {
        types: displayTypes
      };
    } else {
      this.state = {
        types: [
          'string'
        ]
      };
    }
    setTimeout(() => {
      props.onChange(this.state.types);
    });
    this.onTypeChange = this.onTypeChange.bind(this);
  }
  onTypeChange(e) {
    this.setState({
      types: [e.target.value]
    }, () => {
      this.onChange(this.state.types[0]);
    });
  }
  onChange(typeState) {
    this.props.onChange([
      typeState
    ]);
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
                    value={type}
                    onChange={this.onTypeChange.bind(this)}
                  />
                  <div className="field-type"></div>
                  <div className="field-isnull text-center"> TBD </div>
                  {
                    checkComplexType(type) ?
                      <AbstractSchemaRow
                        row={type}
                        onChange={this.onChange.bind(this)}
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
  row: PropTypes.any,
  onChange: PropTypes.func.isRequired
};
