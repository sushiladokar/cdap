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
import {Input} from 'reactstrap';
require('./EnumSchemaRow.less');

export default class EnumSchemaRow extends Component {
  constructor(props) {
    super(props);
    if (props.row.type) {
      let rowType = parseType(props.row.type);
      let symbols = rowType.type.getSymbols();
      this.state = {
        symbols
      };
    } else {
      this.state = {
        symbols: ['']
      };
    }
  }
  onSymbolChange(index, e) {
    let symbols = this.state.symbols;
    symbols[index] = e.target.value;
    this.setState({
      symbols
    }, () => {
      this.props.onChange({
        type: 'enum',
        symbols
      });
    });
  }

  render() {
    return (
      <div className="enum-schema-row">
        <div className="enum-schema-symbols-row">
          {
            this.state.symbols.map((symbol, index) => {
              return (
                <Input
                  key={index}
                  value={symbol}
                  onChange={this.onSymbolChange.bind(this, index)}
                />
              );
            })
          }
        </div>
      </div>
    );
  }
}

EnumSchemaRow.propTypes = {
  row: PropTypes.any,
  onChange: PropTypes.func.isRequired
};