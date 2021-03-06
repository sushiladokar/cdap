/*
 * Copyright © 2017 Cask Data, Inc.
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
import T from 'i18n-react';
import PlusButtonStore from 'services/PlusButtonStore';
require('./NoEntitiesMessage.scss');

export default function NoEntitiesMessage({searchText, filtersAreApplied, clearSearchAndFilters}) {
  const openAddEntityModal = () => {
    PlusButtonStore.dispatch({
      type: 'TOGGLE_PLUSBUTTON_MODAL',
      payload: {
        modalState: true
      }
    });
  };

  let emptyMessage = T.translate('features.EntityListView.emptyMessage.default');
  let clearText;

  if (searchText) {
    emptyMessage = T.translate('features.EntityListView.emptyMessage.search', {searchText});
    clearText = T.translate('features.EntityListView.emptyMessage.clearText.search');
  } else if (filtersAreApplied && filtersAreApplied()) {
    emptyMessage = T.translate('features.EntityListView.emptyMessage.filter');
    clearText = T.translate('features.EntityListView.emptyMessage.clearText.filter');
  }

  return (
    <div className="empty-message-container">
      <strong>{emptyMessage}</strong>
      <hr />
      <div className="empty-message-suggestions">
        {
          clearText ?
            (
              <span>
                <span>{T.translate('features.EntityListView.emptyMessage.suggestion')}</span>
                <br />
                <span
                  className="action-item clear"
                  onClick={clearSearchAndFilters}
                >
                  {T.translate('features.EntityListView.emptyMessage.clearText.clear')}
                </span>
                <span>{clearText}</span>
                <br />
              </span>
            )
          :
            null
        }
        <span
          className="action-item add-entity"
          onClick={openAddEntityModal}
        >
          {T.translate('features.EntityListView.emptyMessage.clearText.add')}
        </span>
        <span>{T.translate('features.EntityListView.emptyMessage.clearText.entities')}</span>
      </div>
    </div>
  );
}
NoEntitiesMessage.propTypes = {
  searchText: PropTypes.string,
  filtersAreApplied: PropTypes.func,
  clearSearchAndFilters: PropTypes.func
};
