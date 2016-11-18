#!/bin/bash
#
# Copyright © 2015-2016 Cask Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

# Ensure SSH is set to startup
if [[ $(which update-rc.d &>/dev/null) ]]; then
  update-rc.d ssh defaults
elif [[ $(which chkconfig &>/dev/null) ]]; then
  chkconfig sshd on
fi

# Delete SSH keys
rm -f /etc/ssh/*_key /etc/ssh/*_key.pub
rm -f /root/.ssh/authorized_keys* /home/*/.ssh/authorized_keys*

exit 0
