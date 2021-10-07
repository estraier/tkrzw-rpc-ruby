#! /usr/bin/ruby -I. -w
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Ruby client library of Tkrzw-RPC
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
#++
#:include:overview.rd

require 'grpc'
require 'tkrzw_rpc_pb'
require 'tkrzw_rpc_services_pb'


# Namespace of Tkrzw-RPC.
module TkrzwRPC
  # Status of operations.
  class Status
    # Success.
    SUCCESS = 0
    # Generic error whose cause is unknown.
    UNKNOWN_ERROR = 1
    # Generic error from underlying systems.
    SYSTEM_ERROR = 2
    # Error that the feature is not implemented.
    NOT_IMPLEMENTED_ERROR = 3
    # Error that a precondition is not met.
    PRECONDITION_ERROR = 4
    # Error that a given argument is invalid.
    INVALID_ARGUMENT_ERROR = 5
    # Error that the operation is canceled.
    CANCELED_ERROR = 6
    # Error that a specific resource is not found.
    NOT_FOUND_ERROR = 7
    # Error that the operation is not permitted.
    PERMISSION_ERROR = 8
    # Error that the operation is infeasible.
    INFEASIBLE_ERROR = 9
    # Error that a specific resource is duplicated.
    DUPLICATION_ERROR = 10
    # Error that internal data are broken.
    BROKEN_DATA_ERROR = 11
    # Error caused by networking failure.
    NETWORK_ERROR = 12
    # Generic error caused by the application logic.
    APPLICATION_ERROR = 13

    # Sets the code and the message.
    # @param code The status code.  This can be omitted and then SUCCESS is set.
    # @param message An arbitrary status message.  This can be omitted and the an empty string is set.
    def initialize(code=SUCCESS, message="")
      @code = code
      @message = message
    end

    # Sets the code and the message.
    # @param code The status code.  This can be omitted and then SUCCESS is set.
    # @param message An arbitrary status message.  This can be omitted and the an empty string is set.
    def set(code=SUCCESS, message="")
      @code = code
      @message = message
    end

    # Assigns the internal state from another status object only if the current state is success.
    # @param rhs The status object.
    def join(rht)
      if @code == SUCCESS
        @code = rht.code
        @message = rht.message
      end
    end

    # Gets the status code.
    # @return The status code.
    def code
      @code
    end

    # Gets the status message.
    # @return The status message.
    def message
      @message
    end

    # Returns true if the status is success.
    # @return True if the status is success, or False on failure.
    def ok?
      @code == SUCCESS
    end

    # Raises an exception if the status is not success.
    # @raise StatusException An exception containing the status object.
    def or_die
      if @code != SUCCESS
        raise StatusException.new(self)
      end
    end

    # Returns a string representation of the content.
    # @return The string representation of the content.
    def to_s
      expr = Status.code_name(@code)
      if not @message.empty?
        expr += ": " + @message
      end
      expr
    end

    # Returns the status code.
    # @return The status code.
    def to_i
      @code
    end

    # Returns a string representation of the object.
    # @return The string representation of the object.
    def inspect
      "#<TkrzwRPC::Status: " + to_s + ">"
    end

    # Returns True if the other object has the same code.
    # @param rhs The object to compare.  It can be a status or an integer.
    # @return True if they are the same, or False if they are not.
    def ==(rhs)
      if rhs.is_a?(Status)
        return @code == rhs.code
      end
      if rhs.is_a?(Integer)
        return @code == rhs
      end
      false
    end

    # Returns True if the other object doesn't have the same code.
    # @param rhs The object to compare.  It can be a status or an integer.
    # @return False if they are the same, or True if they are not.
    def !=(rhs)
      not (self == rhs)
    end

    # Gets the string name of a status code.
    # @param code The status code.
    # @return The name of the status code.
    def self.code_name(code)
      case code
      when SUCCESS
        return "SUCCESS"
      when UNKNOWN_ERROR
        return "UNKNOWN_ERROR"
      when SYSTEM_ERROR
        return "SYSTEM_ERROR"
      when NOT_IMPLEMENTED_ERROR
        return "NOT_IMPLEMENTED_ERROR"
      when PRECONDITION_ERROR
        return "PRECONDITION_ERROR"
      when INVALID_ARGUMENT_ERROR
        return "INVALID_ARGUMENT_ERROR"
      when CANCELED_ERROR
        return "CANCELED_ERROR"
      when NOT_FOUND_ERROR
        return "NOT_FOUND_ERROR"
      when PERMISSION_ERROR
        return "PERMISSION_ERROR"
      when INFEASIBLE_ERROR
        return "INFEASIBLE_ERROR"
      when DUPLICATION_ERROR
        return "DUPLICATION_ERROR"
      when BROKEN_DATA_ERROR
        return "BROKEN_DATA_ERROR"
      when NETWORK_ERROR
        return "NETWORK_ERROR"
      when APPLICATION_ERROR
        return "APPLICATION_ERROR"
      end
      return "unknown"
    end
  end

  # Exception to convey the status of operations.
  class StatusException < RuntimeError
    # Sets the status.
    # @param status The status object.
    def initialize(status)
      @status = status
    end

    # Returns a string representation of the content.
    # @return The string representation of the content.
    def to_s
      @status.to_s
    end

    # Returns a string representation of the object.
    # @return The string representation of the object.
    def inspect
      "#<TkrzwRPC::StatusException: " + to_s + ">"
    end

    # Gets the status object
    # @return The status object.
    def status
      @status
    end
  end

  # Remote database manager.
  # All operations except for "connect" and "disconnect" are thread-safe; Multiple threads can access the same database concurrently.
  class RemoteDBM
    include TkrzwRPC
    attr_reader :channel, :stub, :timeout, :dbm_index, :encoding
    
    # Does nothing especially.
    def initialize
      @channel = nil
      @stub = nil
      @timeout = nil
      @dbm_index = 0
      @encoding = nil
    end

    # Releases the resource explicitly.
    def destruct
      if @channel
        disconnect
      end
      @channel = nil
      @stub = nil
    end

    # Connects to the server.
    # @param address The address or the host name of the server and its port number.  For IPv4 address, it's like "127.0.0.1:1978".  For IPv6, it's like "[::1]:1978".  For UNIX domain sockets, it's like "unix:/path/to/file".
    # @param timeout The timeout in seconds for connection and each operation.  Negative means unlimited.
    # @return The result status.
    def connect(address, timeout=nil)
      if @channel
        return Status.new(Status::PRECONDITION_ERROR, "opened connection")
      end
      timeout = timeout == nil ? 1 << 27 : timeout
      begin
        channel = GRPC::ClientStub.setup_channel(nil, address, :this_channel_is_insecure)
        deadline = Time.now + timeout
        num_retries = 0
        while true do
          if Time.now > deadline
            channel.close
            return Status.new(Status::PRECONDITION_ERROR, "connection timeout")
          end
          state = channel.connectivity_state(true)
          if state == GRPC::Core::ConnectivityStates::READY
            break
          end
          if state == GRPC::Core::ConnectivityStates::TRANSIENT_FAILURE or
            state == GRPC::Core::ConnectivityStates::FATAL_FAILURE
            if num_retries >= 3
              channel.close
              return Status.new(Status::PRECONDITION_ERROR, "connection failed")
            end
            num_retries += 1
          end
          channel.watch_connectivity_state(state, Time.now + 0.1) 
        end
        stub = DBMService::Stub.new(address, :this_channel_is_insecure,
                                    channel_override: @channel, timeout: timeout)
      rescue GRPC::BadStatus => error
        return Status.new(Status::NETWORK_ERROR, str_grpc_error(error))
      end
      @channel = channel
      @stub = stub
      @timeout = timeout
      @dbm_index = 0
      @encoding = nil
      return Status.new(Status::SUCCESS)
    end

    # Disconnects the connection to the server.
    # @return The result status.
    def disconnect
      if not @channel
        return Status.new(Status::PRECONDITION_ERROR, "not opened connection")
      end
      status = Status.new(Status::SUCCESS)
      begin
        @channel.close
      rescue GRPC::BadStatus => error
        status = Status.new(Status::NETWORK_ERROR, str_grpc_error(error))
      end
      @channel = nil
      @stub = nil
      @timeout = nil
      @dbm_index = 0
      @encoding = nil
      return status
    end

    # Sets the index of the DBM to access.
    # @param dbm_index The index of the DBM to access.
    # @return The result status.
    def set_dbm_index(dbm_index)
      if not @channel
        return Status.new(Status::PRECONDITION_ERROR, "not opened connection")
      end
      @dbm_index = dbm_index
      Status.new(Status::SUCCESS)
    end

    # Sets the encoding of string values returned by some methods like Get.
    # @param encoding The encoding name like "UTF-8", "ISO-8859-1" and "ASCII-8BIT".
    # @return The result status.
    # The default encoding is "ASCII-8BIT" which is suitable for binary data.
    def set_encoding(encoding)
      if not @channel
        return Status.new(Status::PRECONDITION_ERROR, "not opened connection")
      end
      @encoding = encoding == "ASCII-8BIT" ? nil : encoding
      Status.new(Status::SUCCESS)
    end

    # Inspects the database.
    # @return A hash of property names and their values.
    def inspect_details
      result = {}
      if not @channel
        return result
      end
      request = InspectRequest.new
      request.dbm_index = @dbm_index
      begin
        response = @stub.inspect(request)
      rescue GRPC::BadStatus
        return result
      end
      for record in response.records
        result[record.first] = record.second
      end
      result
    end
       
    # Sends a message and gets back the echo message.
    # @param message The message to send.
    # @param status A status object to which the result status is assigned.  It can be omitted.
    # @return The string value of the echoed message or nil on failure.
    def echo(message, status=nil)
      if not @channel
        if status
          status.set(Status::PRECONDITION_ERROR, "not opened connection")
        end
        return nil
      end
      request = EchoRequest.new
      request.message = make_string(message)
      begin
        response = @stub.echo(request)
      rescue GRPC::BadStatus => error
        if status
          status.set(Status::NETWORK_ERROR, str_grpc_error(error))
        end
        return nil
      end
      if status
        status.set(Status::SUCCESS)
      end
      if @encoding
        return response.echo.dup.force_encoding(@encoding)
      end
      response.echo
    end

    # Gets the value of a record of a key.
    # @param key The key of the record.
    # @param status A status object to which the result status is assigned.  It can be omitted.
    # @return The value of the matching record or nil on failure.
    def get(key, status=nil)
      if not @channel
        if status
          status.set(Status::PRECONDITION_ERROR, "not opened connection")
        end
        return nil
      end
      request = GetRequest.new
      request.dbm_index = @dbm_index
      request.key = make_string(key)
      begin
        response = @stub.get(request)
      rescue GRPC::BadStatus => error
        if status
          status.set(Status::NETWORK_ERROR, str_grpc_error(error))
        end
        return nil
      end
      if status
        set_status_from_proto(status, response.status)
      end
      if response.status.code == Status::SUCCESS
        if @encoding
          return response.value.dup.force_encoding(@encoding)
        end
        return response.value
      end
      nil
    end

    # Gets the values of multiple records of keys.
    # @param keys The keys of records to retrieve.
    # @return A map of retrieved records.  Keys which don't match existing records are ignored.
    def get_multi(*keys)
      result = {}
      if not @channel
        return result
      end
      request = GetMultiRequest.new
      request.dbm_index = @dbm_index
      keys.each { |key|
        request.keys.push(make_string(key))
      }
      begin
        response = @stub.get_multi(request)
      rescue GRPC::BadStatus => error
        print(error)
        return {}
      end
      response.records.each { |record|
        if @encoding
          result[record.first.dup.force_encoding(@encoding)] =
            record.second.dup.force_encoding(@encoding)
        else
          result[record.first] = record.second
        end
      }
      result
    end

    # Sets a record of a key and a value.
    # @param key The key of the record.
    # @param value The value of the record.
    # @param overwrite Whether to overwrite the existing value.  It can be omitted and then false is set.
    # @return The result status.  If overwriting is abandoned, DUPLICATION_ERROR is returned.
    def set(key, value, overwrite=true)
      if not @channel
        return Status.new(Status::PRECONDITION_ERROR, "not opened connection")
      end
      request = SetRequest.new
      request.dbm_index = @dbm_index
      request.key = make_string(key)
      request.value = make_string(value)
      request.overwrite = overwrite
      begin
        response = @stub.set(request)
      rescue GRPC::BadStatus => error
        return Status.new(Status::NETWORK_ERROR, str_grpc_error(error))
      end
      make_status_from_proto(response.status)
    end

    # Sets multiple records of the keyword arguments.
    # @param overwrite Whether to overwrite the existing value if there's a record with the same key.  If true, the existing value is overwritten by the new value.  If false, the operation is given up and an error status is returned.
    # @param records Records to store.
    # @return The result status.  If there are records avoiding overwriting, DUPLICATION_ERROR is returned.
    def set_multi(overwrite=true, **records)
      if not @channel
        return Status.new(Status::PRECONDITION_ERROR, "not opened connection")
      end
      request = SetMultiRequest.new
      request.dbm_index = @dbm_index
      records.each { |key, value|
        req_record = BytesPair.new
        req_record.first = make_string(key)
        req_record.second = make_string(value)
        request.records.push(req_record)
      }
      request.overwrite = overwrite
      begin
        response = @stub.set_multi(request)
      rescue GRPC::BadStatus => error
        return Status.new(Status::NETWORK_ERROR, str_grpc_error(error))
      end
      make_status_from_proto(response.status)
    end

    # Removes a record of a key.
    # @param key The key of the record.
    # @return The result status.  If there's no matching record, NOT_FOUND_ERROR is returned.
    def remove(key)
      if not @channel
        return Status.new(Status::PRECONDITION_ERROR, "not opened connection")
      end
      request = RemoveRequest.new
      request.dbm_index = @dbm_index
      request.key = make_string(key)
      begin
        response = @stub.remove(request)
      rescue GRPC::BadStatus => error
        return Status.new(Status::NETWORK_ERROR, str_grpc_error(error))
      end
      make_status_from_proto(response.status)
    end

    # Removes records of keys.
    # @param keys The keys of the records.
    # @return The result status.  If there are missing records, NOT_FOUND_ERROR is returned.
    def remove_multi(*keys)
      if not @channel
        return Status.new(Status::PRECONDITION_ERROR, "not opened connection")
      end
      request = RemoveMultiRequest.new
      request.dbm_index = @dbm_index
      keys.each { |key|
        request.keys.push(make_string(key))
      }
      begin
        response = @stub.remove_multi(request)
      rescue GRPC::BadStatus => error
        return Status.new(Status::NETWORK_ERROR, str_grpc_error(error))
      end
      make_status_from_proto(response.status)
    end

    # Appends data at the end of a record of a key.
    # @param key The key of the record.
    # @param value The value to append.
    # @param delim The delimiter to put after the existing record.
    # @return The result status.
    # If there's no existing record, the value is set without the delimiter.
    def append(key, value, delim="")
      if not @channel
        return Status.new(Status::PRECONDITION_ERROR, "not opened connection")
      end
      request = AppendRequest.new
      request.dbm_index = @dbm_index
      request.key = make_string(key)
      request.value = make_string(value)
      request.delim = make_string(delim)
      begin
        response = @stub.append(request)
      rescue GRPC::BadStatus => error
        return Status.new(Status::NETWORK_ERROR, str_grpc_error(error))
      end
      make_status_from_proto(response.status)
    end

    # Appends data to multiple records of the keyword arguments.
    # @param delim The delimiter to put after the existing record.
    # @param records Records to append.
    # @return The result status.
    def append_multi(delim="", **records)
      if not @channel
        return Status.new(Status::PRECONDITION_ERROR, "not opened connection")
      end
      request = AppendMultiRequest.new
      request.dbm_index = @dbm_index
      records.each { |key, value|
        req_record = BytesPair.new
        req_record.first = make_string(key)
        req_record.second = make_string(value)
        request.records.push(req_record)
      }
      request.delim = make_string(delim)
      begin
        response = @stub.append_multi(request)
      rescue GRPC::BadStatus => error
        return Status.new(Status::NETWORK_ERROR, str_grpc_error(error))
      end
      make_status_from_proto(response.status)
    end

    # Compares the value of a record and exchanges if the condition meets.
    # @param key The key of the record.
    # @param expected The expected value.  If it is None, no existing record is expected.
    # @param desired The desired value.  If it is nil, the record is to be removed.
    # @return The result status.  If the condition doesn't meet, INFEASIBLE_ERROR is returned.
    def compare_exchange(key, expected, desired)
      if not @channel
        return Status.new(Status::PRECONDITION_ERROR, "not opened connection")
      end
      request = CompareExchangeRequest.new
      request.dbm_index = @dbm_index
      request.key = make_string(key)
      if expected != nil
        request.expected_existence = true
        request.expected_value = make_string(expected)
      end
      if desired != nil
        request.desired_existence = true
        request.desired_value = make_string(desired)
      end
      begin
        response = @stub.compare_exchange(request)
      rescue GRPC::BadStatus => error
        return Status.new(Status::NETWORK_ERROR, str_grpc_error(error))
      end
      make_status_from_proto(response.status)
    end

    # Increments the numeric value of a record.
    # @param key The key of the record.
    # @param inc The incremental value.  If it is Utility::INT64MIN, the current value is not changed and a new record is not created.
    # @param init The initial value.
    # @param status A status object to which the result status is assigned.  It can be omitted.
    # @return The current value, or nil on failure.
    # The record value is stored as an 8-byte big-endian integer.  Negative is also supported.
    def increment(key, inc=1, init=0, status=nil)
      if not @channel
        if status
          status.set(Status::PRECONDITION_ERROR, "not opened connection")
        end
        return nil
      end
      request = IncrementRequest.new
      request.dbm_index = @dbm_index
      request.key = make_string(key)
      request.increment = inc
      request.initial = init
      begin
        response = @stub.increment(request)
      rescue GRPC::BadStatus => error
        if status
          status.set(Status::NETWORK_ERROR, str_grpc_error(error))
        end
        return nil
      end
      if status
        set_status_from_proto(status, response.status)
      end
      if response.status.code == Status::SUCCESS
        return response.current
      end
      nil
    end

    # Compares the values of records and exchanges if the condition meets.
    # @param expected An array of pairs of the record keys and their expected values.  If the value is nil, no existing record is expected.
    # @param desired An array of pairs of the record keys and their desired values.  If the value is nil, the record is to be removed.
    # @return The result status.  If the condition doesn't meet, INFEASIBLE_ERROR is returned.
    def compare_exchange_multi(expected, desired)
      if not @channel
        return Status.new(Status::PRECONDITION_ERROR, "not opened connection")
      end
      request = CompareExchangeMultiRequest.new
      request.dbm_index = @dbm_index
      expected.each { |elem|
        state = RecordState.new
        state.key = make_string(elem[0])
        if elem[1] != nil
          state.existence = true
          state.value = make_string(elem[1])
        end
        request.expected.push(state)
      }
      desired.each { |elem|
        state = RecordState.new
        state.key = make_string(elem[0])
        if elem[1] != nil
          state.existence = true
          state.value = make_string(elem[1])
        end
        request.desired.push(state)
      }
      begin
        response = @stub.compare_exchange_multi(request)
      rescue GRPC::BadStatus => error
        return Status.new(Status::NETWORK_ERROR, str_grpc_error(error))
      end
      make_status_from_proto(response.status)
    end
    
    # Gets the number of records.
    # @return The number of records on success, or nil on failure.
    def count
      if not @channel
        return nil
      end
      request = CountRequest.new
      request.dbm_index = @dbm_index
      begin
        response = @stub.count(request)
      rescue GRPC::BadStatus
        return nil
      end
      response.count
    end
  
    # Gets the current file size of the database.
    # @return The current file size of the database, or nil on failure.
    def file_size
      if not @channel
        return nil
      end
      request = GetFileSizeRequest.new
      request.dbm_index = @dbm_index
      begin
        response = @stub.get_file_size(request)
      rescue GRPC::BadStatus
        return nil
      end
      response.file_size
    end
    
    # Removes all records.
    # @return The result status.
    def clear
      if not @channel
        return Status.new(Status::PRECONDITION_ERROR, "not opened connection")
      end
      request = ClearRequest.new
      request.dbm_index = @dbm_index
      begin
        response = @stub.clear(request)
      rescue GRPC::BadStatus => error
        return Status.new(Status::NETWORK_ERROR, str_grpc_error(error))
      end
      make_status_from_proto(response.status)
    end

    # Rebuilds the entire database.
    # @param params Optional parameters of a hash object.
    # @return The result status.
    # The optional parameters are the same as the "open" method of the local DBM class and the database configurations of the server command.  Omitted tuning parameters are kept the same or implicitly optimized.<br>
    # In addition, HashDBM, TreeDBM, and SkipDBM supports the following parameters.
    # - skip_broken_records (bool): If true, the operation continues even if there are broken records which can be skipped.
    # - sync_hard (bool): If true, physical synchronization with the hardware is done before finishing the rebuilt file.
    def rebuild(**params)
      if not @channel
        return Status.new(Status::PRECONDITION_ERROR, "not opened connection")
      end
      request = RebuildRequest.new
      request.dbm_index = @dbm_index
      params.each { |name, value|
        req_param = StringPair.new
        req_param.first = make_string(name)
        req_param.second = make_string(value)
        request.params.push(req_param)
      }
      begin
        response = @stub.rebuild(request)
      rescue GRPC::BadStatus => error
        return Status.new(Status::NETWORK_ERROR, str_grpc_error(error))
      end
      make_status_from_proto(response.status)
    end

    # Checks whether the database should be rebuilt.
    # @return True to be optimized or False with no necessity.
    def should_be_rebuilt?
      if not @channel
        return false
      end
      request = ShouldBeRebuiltRequest.new
      request.dbm_index = @dbm_index
      begin
        response = @stub.should_be_rebuilt(request)
      rescue GRPC::BadStatus
        return false
      end
      response.tobe
    end

    # Synchronizes the content of the database to the file system.
    # @param hard True to do physical synchronization with the hardware or false to do only logical synchronization with the file system.
    # @param params Optional parameters of a hash object.
    # @return The result status.
    # Only SkipDBM uses the optional parameters.  The "merge" parameter specifies paths of databases to merge, separated by colon.  The "reducer" parameter specifies the reducer to apply to records of the same key.  "ReduceToFirst", "ReduceToSecond", "ReduceToLast", etc are supported.
    def synchronize(hard, **params)
      if not @channel
        return Status.new(Status::PRECONDITION_ERROR, "not opened connection")
      end
      request = SynchronizeRequest.new
      request.dbm_index = @dbm_index
      request.hard = hard
      params.each { |name, value|
        req_param = StringPair.new
        req_param.first = make_string(name)
        req_param.second = make_string(value)
        request.params.push(req_param)
      }
      begin
        response = @stub.synchronize(request)
      rescue GRPC::BadStatus => error
        return Status.new(Status::NETWORK_ERROR, str_grpc_error(error))
      end
      make_status_from_proto(response.status)
    end

    # Searches the database and get keys which match a pattern.
    # @param mode The search mode.  "contain" extracts keys containing the pattern.  "begin" extracts keys beginning with the pattern.  "end" extracts keys ending with the pattern.  "regex" extracts keys partially matches the pattern of a regular expression.  "edit" extracts keys whose edit distance to the UTF-8 pattern is the least.  "editbin" extracts keys whose edit distance to the binary pattern is the least.
    # @param pattern The pattern for matching.
    # @param capacity The maximum records to obtain.  0 means unlimited.
    # @return A list of keys matching the condition.
    def search(mode, pattern, capacity=0)
      result = []
      if not @channel
        result
      end
      request = SearchRequest.new
      request.dbm_index = @dbm_index
      request.mode =make_string(mode)
      request.pattern =make_string(pattern)
      request.capacity = capacity
      begin
        response = @stub.search(request)
      rescue GRPC::BadStatus
        return result
      end
      if response.status.code == Status::SUCCESS
        if @encoding
          response.matched.each { |key|
            result.push(key.dup.force_encoding(@encoding))
          }
        else
          response.matched.each { |key|
            result.push(key)
          }
        end
      end
      result
    end
  
    # Makes an iterator for each record.
    # @return The iterator for each record.
    # Every iterator should be destructed explicitly by the "destruct" method.
    def make_iterator
      return Iterator.new(self)
    end

    # Returns a string representation of the content.
    # @return The string representation of the content.
    def to_s
      expr = @channel ? "connected" : "not connected"
      "RemoteDBM: 0x" + object_id.to_s(16) + ": " + expr
    end

    # Gets the number of records.
    # @return The number of records on success, or -1 on failure.
    def to_i
      if not @channel
        return -1
      end
      request = CountRequest.new
      request.dbm_index = @dbm_index
      begin
        response = @stub.count(request)
      rescue GRPC::BadStatus => error
        return Status.new(Status::NETWORK_ERROR, str_grpc_error(error))
      end
      response.count
    end

    # Returns a string representation of the object.
    # @return The string representation of the object.
    def inspect
      expr = @channel ? "connected" : "not connected"
      "#<TkrzwRPC::RemoteDBM: 0x" + object_id.to_s(16) + ": " + expr + ">"
    end

    # Gets the value of a record, to enable the [] operator.
    # @param key The key of the record.
    # @return The value of the matching record or nil on failure.
    def [](key)
      if not @channel
        return nil
      end
      request = GetRequest.new
      request.dbm_index = @dbm_index
      request.key = make_string(key)
      begin
        response = @stub.get(request)
      rescue GRPC::BadStatus
        return nil
      end
      if response.status.code == Status::SUCCESS
        if @encoding
          return response.value.dup.force_encoding(@encoding)
        end
        return response.value
      end
      nil
    end

    # Sets a record of a key and a value, to enable the []= operator.
    # @param key The key of the record.
    # @param value The value of the record.
    # @return The new value of the record or nil on failure.
    def []=(key, value)
      if not @channel
        return nil
      end
      request = SetRequest.new
      request.dbm_index = @dbm_index
      request.key = make_string(key)
      request.value = make_string(value)
      request.overwrite = true
      begin
        response = @stub.set(request)
      rescue GRPC::BadStatus
        return nil
      end
      if response.status.code == Status::SUCCESS
        if @encoding
          return request.value.dup.force_encoding(@encoding)
        end
        return request.value
      end
      nil
    end

    # Removes a record of a key.
    # @param key The key of the record.
    # @return an empty string on success or nil on failure.
    def delete(key)
      if not @channel
        return false
      end
      request = RemoveRequest.new
      request.dbm_index = @dbm_index
      request.key = make_string(key)
      begin
        response = @stub.remove(request)
      rescue GRPC::BadStatus
        return false
      end
      response.status.code == Status::SUCCESS ? "" : nil
    end

    # Calls the given block with the key and the value of each record
    def each(&block)
      iter = make_iterator
      begin
        iter.first
        while true
          record = iter.get
          if not record
            break
          end
          yield record[0], record[1]
          iter.next
        end
      ensure
        iter.destruct
      end
    end
  end

  class Event
    def initialize
      @mutex = Mutex.new
      @cond = ConditionVariable.new
      @is_set = false
    end
    def set
      @mutex.synchronize {
        @is_set = true
      }
      @cond.signal
    end
    def clear
      @mutex.synchronize {
        @is_set = false
      }
    end
    def wait
      @mutex.synchronize {
        while not @is_set do
          @cond.wait(@mutex)          
        end
      }
    end
  end

  class RequestIterator
    attr_reader :event, :request
    attr_writer :event, :request
    def initialize
      @event = Event.new
      @request = nil
    end
    def each_item
      return enum_for(:each_item) unless block_given?
      loop {
        @event.wait
        @event.clear
        if @request
          yield @request
        end
      }
    end
  end
        
  # Iterator for each record.
  # An iterator is made by the "make_iterator" method of DBM.  Every unused iterator object should be destructed explicitly by the "destruct" method to free resources.
  class Iterator
    include TkrzwRPC
    
    # Initializes the iterator.
    # @param dbm The database to scan.
    def initialize(dbm)
      if not dbm.channel
        raise StatusException.new(Status.new(Status.PRECONDITION_ERROR, "not opened connection"))
      end
      @dbm = dbm
      @req_it = RequestIterator.new
      begin
        @res_it = dbm.stub.iterate(@req_it.each_item)
      rescue GRPC::BadStatus
        @dbm = None
        @req_it = None
      end
    end

    # Releases the resource explicitly.
    def destruct
      @req_it.request = nil
      @req_it.event.set
      @dbm = nil
      @req_it = nil
      @res_it = nil
    end

    # Initializes the iterator to indicate the first record.
    # @return The result status.
    # Even if there's no record, the operation doesn't fail.
    def first
      request = IterateRequest.new
      request.dbm_index = @dbm.dbm_index
      request.operation = IterateRequest::OpType::OP_FIRST
      begin
        @req_it.request = request
        @req_it.event.set
        response = @res_it.next
        @req_it.request = nil
      rescue GRPC::BadStatus => error
        return Status.new(Status::NETWORK_ERROR, str_grpc_error(error))
      end
      return make_status_from_proto(response.status)
    end
  
    # Initializes the iterator to indicate the last record.
    # @return The result status.
    # Even if there's no record, the operation doesn't fail.  This method is suppoerted only by ordered databases.
    def last
      request = IterateRequest.new
      request.dbm_index = @dbm.dbm_index
      request.operation = IterateRequest::OpType::OP_LAST
      begin
        @req_it.request = request
        @req_it.event.set
        response = @res_it.next
        @req_it.request = nil
      rescue GRPC::BadStatus => error
        return Status.new(Status::NETWORK_ERROR, str_grpc_error(error))
      end
      return make_status_from_proto(response.status)
    end

    # Initializes the iterator to indicate a specific record.
    # @param key The key of the record to look for.
    # @return The result status.
    # Ordered databases can support "lower bound" jump; If there's no record with the same key, the iterator refers to the first record whose key is greater than the given key.  The operation fails with unordered databases if there's no record with the same key.
    def jump(key)
      request = IterateRequest.new
      request.dbm_index = @dbm.dbm_index
      request.operation = IterateRequest::OpType::OP_JUMP
      request.key = make_string(key)
      begin
        @req_it.request = request
        @req_it.event.set
        response = @res_it.next
        @req_it.request = nil
      rescue GRPC::BadStatus => error
        return Status.new(Status::NETWORK_ERROR, str_grpc_error(error))
      end
      return make_status_from_proto(response.status)
    end

    # Initializes the iterator to indicate the last record whose key is lower than a given key.
    # @param key The key to compare with.
    # @param inclusive If true, the considtion is inclusive: equal to or lower than the key.
    # @return The result status.
    # Even if there's no matching record, the operation doesn't fail.  This method is suppoerted only by ordered databases.
    def jump_lower(key, inclusive=false)
      request = IterateRequest.new
      request.dbm_index = @dbm.dbm_index
      request.operation = IterateRequest::OpType::OP_JUMP_LOWER
      request.key = make_string(key)
      request.jump_inclusive = inclusive
      begin
        @req_it.request = request
        @req_it.event.set
        response = @res_it.next
        @req_it.request = nil
      rescue GRPC::BadStatus => error
        return Status.new(Status::NETWORK_ERROR, str_grpc_error(error))
      end
      return make_status_from_proto(response.status)
    end

    # Initializes the iterator to indicate the first record whose key is upper than a given key.
    # @param key The key to compare with.
    # @param inclusive If true, the considtion is inclusive: equal to or upper than the key.
    # @return The result status.
    # Even if there's no matching record, the operation doesn't fail.  This method is suppoerted only by ordered databases.
    def jump_upper(key, inclusive=false)
      request = IterateRequest.new
      request.dbm_index = @dbm.dbm_index
      request.operation = IterateRequest::OpType::OP_JUMP_UPPER
      request.key = make_string(key)
      request.jump_inclusive = inclusive
      begin
        @req_it.request = request
        @req_it.event.set
        response = @res_it.next
        @req_it.request = nil
      rescue GRPC::BadStatus => error
        return Status.new(Status::NETWORK_ERROR, str_grpc_error(error))
      end
      return make_status_from_proto(response.status)
    end
  
    # Moves the iterator to the next record.
    # @return The result status.
    # If the current record is missing, the operation fails.  Even if there's no next record, the operation doesn't fail.
    def next
      request = IterateRequest.new
      request.dbm_index = @dbm.dbm_index
      request.operation = IterateRequest::OpType::OP_NEXT
      begin
        @req_it.request = request
        @req_it.event.set
        response = @res_it.next
        @req_it.request = nil
      rescue GRPC::BadStatus => error
        return Status.new(Status::NETWORK_ERROR, str_grpc_error(error))
      end
      return make_status_from_proto(response.status)
    end

    # Moves the iterator to the previous record.
    # @return The result status.
    # If the current record is missing, the operation fails.  Even if there's no previous record, the operation doesn't fail.  This method is suppoerted only by ordered databases.
    def previous
      request = IterateRequest.new
      request.dbm_index = @dbm.dbm_index
      request.operation = IterateRequest::OpType::OP_PREVIOUS
      begin
        @req_it.request = request
        @req_it.event.set
        response = @res_it.next
        @req_it.request = nil
      rescue GRPC::BadStatus => error
        return Status.new(Status::NETWORK_ERROR, str_grpc_error(error))
      end
      return make_status_from_proto(response.status)
    end

    # Gets the key and the value of the current record of the iterator.
    # @param status A status object to which the result status is assigned.  It can be omitted.
    # @return A tuple of The key and the value of the current record.  On failure, nil is returned.
    def get(status=nil)
      request = IterateRequest.new
      request.dbm_index = @dbm.dbm_index
      request.operation = IterateRequest::OpType::OP_GET
      begin
        @req_it.request = request
        @req_it.event.set
        response = @res_it.next
        @req_it.request = nil
      rescue GRPC::BadStatus => error
        return Status.new(Status::NETWORK_ERROR, str_grpc_error(error))
      end
      if status
        set_status_from_proto(status, response.status)
      end
      if response.status.code == Status::SUCCESS
        if @dbm.encoding
          return [response.key.dup.force_encoding(@dbm.encoding),
                  response.value.dup.force_encoding(@dbm.encoding)]
        end
        return [response.key, response.value]
      end
      nil
    end

    # Gets the key of the current record.
    # @param status A status object to which the result status is assigned.  It can be omitted.
    # @return The key of the current record or nil on failure.
    def get_key(status=nil)
      request = IterateRequest.new
      request.dbm_index = @dbm.dbm_index
      request.operation = IterateRequest::OpType::OP_GET
      request.omit_value = true
      begin
        @req_it.request = request
        @req_it.event.set
        response = @res_it.next
        @req_it.request = nil
      rescue GRPC::BadStatus => error
        return Status.new(Status::NETWORK_ERROR, str_grpc_error(error))
      end
      if status
        set_status_from_proto(status, response.status)
      end
      if response.status.code == Status::SUCCESS
        if @dbm.encoding
          return response.key.dup.force_encoding(@encoding)
        end
        return response.key
      end
      nil
    end

    # Gets the value of the current record.
    # @param status A status object to which the result status is assigned.  It can be omitted.
    # @return The value of the current record or nil on failure.
    def get_value(status=nil)
      request = IterateRequest.new
      request.dbm_index = @dbm.dbm_index
      request.operation = IterateRequest::OpType::OP_GET
      request.omit_key = true
      begin
        @req_it.request = request
        @req_it.event.set
        response = @res_it.next
        @req_it.request = nil
      rescue GRPC::BadStatus => error
        return Status.new(Status::NETWORK_ERROR, str_grpc_error(error))
      end
      if status
        set_status_from_proto(status, response.status)
      end
      if response.status.code == Status::SUCCESS
        if @dbm.encoding
          return response.value.dup.force_encoding(@encoding)
        end
        return response.value
      end
      nil
    end
  
    # Sets the value of the current record.
    # @param value The value of the record.
    # @return The result status.
    def set(value)
      request = IterateRequest.new
      request.dbm_index = @dbm.dbm_index
      request.operation = IterateRequest::OpType::OP_SET
      request.value = value
      begin
        @req_it.request = request
        @req_it.event.set
        response = @res_it.next
        @req_it.request = nil
      rescue GRPC::BadStatus => error
        return Status.new(Status::NETWORK_ERROR, str_grpc_error(error))
      end
      return make_status_from_proto(response.status)
    end

    # Removes the current record.
    # @return The result status.
    def remove
      request = IterateRequest.new
      request.dbm_index = @dbm.dbm_index
      request.operation = IterateRequest::OpType::OP_REMOVE
      begin
        @req_it.request = request
        @req_it.event.set
        response = @res_it.next
        @req_it.request = nil
      rescue GRPC::BadStatus => error
        return Status.new(Status::NETWORK_ERROR, str_grpc_error(error))
      end
      return make_status_from_proto(response.status)
    end

    # Returns a string representation of the content.
    # @return The string representation of the content.
    def to_s
      expr = @dbm.channel ? "connected" : "not connected"
      "Iterator: 0x" + object_id.to_s(16) + ": " + expr
    end

    # Returns a string representation of the object.
    # @return The string representation of the object.
    def inspect
      expr = @dbm.channel ? "connected" : "not connected"
      "#<TkrzwRPC::Iterator: 0x" + object_id.to_s(16) + ": " + expr + ">"
    end
  end

  module_function

  def grpc_code_name(code)
    k = GRPC::Core::StatusCodes.constants.find { |k|
      GRPC::Core::StatusCodes.const_get(k) == code
    }
    k ? k.to_s : "unknonw"
  end

  def str_grpc_error(error)
    code_name = grpc_code_name(error.code)
    details = error.details
    if details and not details.empty?
      return code_name + ": " + details
    end
    code_name
  end

  def make_status_from_proto(proto_status)
    return Status.new(proto_status.code, proto_status.message)
  end

  def set_status_from_proto(status, proto_status)
    status.set(proto_status.code, proto_status.message)
  end

  def make_string(obj)
    str = obj.to_s
    str.force_encoding("ASCII-8BIT")
    str
  end
end


# END OF FILE
