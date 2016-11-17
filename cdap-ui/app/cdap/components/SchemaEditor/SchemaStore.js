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

import {combineReducers, createStore} from 'redux';
const defaultAction = {
  type: '',
  payload: {}
};

const defaultState = {
  name: 'etlSchemabody',
  type: 'record',
  fields: [
    {
      name: '',
      type: {},
      displayType: 'string'
    }
  ]
};

const schema = (state = defaultState, action = defaultAction) => {
  switch(action.type) {
    case 'ADD-ROW':
      return [
        ...state.slice(0, action.payload.index),
        defaultState,
        ...state.slice(action.payload.index, state.length)
      ];
    default:
      return state;
  }
};

let createStoreInstance = () => {
  return createStore(
    combineReducers({
      schema
    }),
    {
      schema: {
        name: 'etlSchemabody',
        type: 'record',
        fields: [
          {
            name: 'Field1',
            displayType: 'string',
            type: 'string'
          },
          {
            name: 'Field2',
            displayType: 'int',
            type: 'int'
          },
          {
            name: 'Field3',
            displayType: 'float',
            type: 'float'
          },
          {
              "name": "Field4",
              "type": {
                  "type": "array",
                  "items": "int"
              }
          },
          {
              "name": "Field5",
              "type": {
                  "type": "array",
                  "items": "int"
              }
          },
          {
              "name": "Field6",
              "type": {
                  "type": "array",
                  "items": "int"
              }
          },
          {
              "name": "Field7",
              "type": {
                  "type": "map",
                  "keys": "string",
                  "values": "string"
              }
          },
          {
              "name": "Field8",
              "type": [
                  "string",
                  "long",
                  "boolean"
              ]
          }
        ]
      }
    }
  );
};

export {createStoreInstance};
let SchemaStore =  createStoreInstance();
export default SchemaStore;
