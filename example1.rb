#! /usr/bin/ruby -I.
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Example for basic usage of the remote database
#
# Copyright 2020 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
# except in compliance with the License.  You may obtain a copy of the License at
#     https://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software distributed under the
# License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied.  See the License for the specific language governing permissions
# and limitations under the License.
#--------------------------------------------------------------------------------------------------

require 'tkrzw_rpc'

# Prepares the database.
dbm = TkrzwRPC::RemoteDBM.new
dbm.connect("localhost:1978")
dbm.clear

# Sets records.
dbm["first"] = "hop"
dbm["second"] = "step"
dbm["third"] = "jump"

# Retrieves record values.
# If the operation fails, nil is returned.
p dbm["first"]
p dbm["second"]
p dbm["third"]
p dbm["fourth"]

# Checks and deletes a record.
if dbm.include?("first")
  dbm.remove("first")
end

# Traverses records.
dbm.each do |key, value|
  p key + ": " + value
end

# Closes and the connection and releases the resources.
dbm.disconnect
dbm.destruct

# END OF FILE
