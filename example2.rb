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

dbm = TkrzwRPC::RemoteDBM.new
begin
  # Prepares the database.
  # The timeout is in seconds.
  status = dbm.connect("localhost:1978", 10)
  if not status.ok?
    raise TkrzwRPC::StatusException.new(status)
  end

  # Sets the index of the database to operate.
  # The default value 0 means the first database on the server.
  # 1 means the second one and 2 means the third one, if any.
  dbm.set_dbm_index(0).or_die

  # Sets records.
  # The method OrDie raises a runtime error on failure.
  dbm.set(1, "hop").or_die
  dbm.set(2, "step").or_die
  dbm.set(3, "jump").or_die

  # Retrieves records without checking errors.
  p dbm.get(1)
  p dbm.get(2)
  p dbm.get(3)
  p dbm.get(4)

  # To know the status of retrieval, give a status object to "get".
  # You can compare a status object and a status code directly.
  status = TkrzwRPC::Status.new
  value = dbm.get(1, status)
  printf("status: %s\n", status)
  if status == TkrzwRPC::Status::SUCCESS
    printf("value: %s\n", value)
  end

  # Rebuilds the database.
  # Optional parameters compatible with the database type can be given.
  dbm.rebuild

  # Traverses records with an iterator.
  begin
    iter = dbm.make_iterator
    iter.first
    while true do
      status = TkrzwRPC::Status.new
      record = iter.get(status)
      break if not status.ok?
      printf("%s: %s\n", record[0], record[1])
      iter.next
    end
  ensure
    # Releases the resources.
    iter.destruct
  end

  # Closes the database.
  dbm.disconnect.or_die
ensure
  # Releases the resources.
  dbm.destruct
end

# END OF FILE
