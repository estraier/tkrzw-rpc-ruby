# Generated by the protocol buffer compiler.  DO NOT EDIT!
# Source: tkrzw_rpc.proto for package 'tkrzw_rpc'
# Original file comments:
# Service definition of Tkrzw-RPC
#
# Copyright 2020 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
# except in compliance with the License.  You may obtain a copy of the License at
#     https://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software distributed under the
# License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied.  See the License for the specific language governing permissions
# and limitations under the License.
#

require 'grpc'
require 'tkrzw_rpc_pb'

module TkrzwRPC
  module DBMService
    # Definition of the database service.
    class Service

      include ::GRPC::GenericService

      self.marshal_class_method = :encode
      self.unmarshal_class_method = :decode
      self.service_name = 'tkrzw_rpc.DBMService'

      rpc :Echo, ::TkrzwRPC::EchoRequest, ::TkrzwRPC::EchoResponse
      rpc :Inspect, ::TkrzwRPC::InspectRequest, ::TkrzwRPC::InspectResponse
      rpc :Get, ::TkrzwRPC::GetRequest, ::TkrzwRPC::GetResponse
      rpc :GetMulti, ::TkrzwRPC::GetMultiRequest, ::TkrzwRPC::GetMultiResponse
      rpc :Set, ::TkrzwRPC::SetRequest, ::TkrzwRPC::SetResponse
      rpc :SetMulti, ::TkrzwRPC::SetMultiRequest, ::TkrzwRPC::SetMultiResponse
      rpc :Remove, ::TkrzwRPC::RemoveRequest, ::TkrzwRPC::RemoveResponse
      rpc :RemoveMulti, ::TkrzwRPC::RemoveMultiRequest, ::TkrzwRPC::RemoveMultiResponse
      rpc :Append, ::TkrzwRPC::AppendRequest, ::TkrzwRPC::AppendResponse
      rpc :AppendMulti, ::TkrzwRPC::AppendMultiRequest, ::TkrzwRPC::AppendMultiResponse
      rpc :CompareExchange, ::TkrzwRPC::CompareExchangeRequest, ::TkrzwRPC::CompareExchangeResponse
      rpc :Increment, ::TkrzwRPC::IncrementRequest, ::TkrzwRPC::IncrementResponse
      rpc :CompareExchangeMulti, ::TkrzwRPC::CompareExchangeMultiRequest, ::TkrzwRPC::CompareExchangeMultiResponse
      rpc :Count, ::TkrzwRPC::CountRequest, ::TkrzwRPC::CountResponse
      rpc :GetFileSize, ::TkrzwRPC::GetFileSizeRequest, ::TkrzwRPC::GetFileSizeResponse
      rpc :Clear, ::TkrzwRPC::ClearRequest, ::TkrzwRPC::ClearResponse
      rpc :Rebuild, ::TkrzwRPC::RebuildRequest, ::TkrzwRPC::RebuildResponse
      rpc :ShouldBeRebuilt, ::TkrzwRPC::ShouldBeRebuiltRequest, ::TkrzwRPC::ShouldBeRebuiltResponse
      rpc :Synchronize, ::TkrzwRPC::SynchronizeRequest, ::TkrzwRPC::SynchronizeResponse
      rpc :Search, ::TkrzwRPC::SearchRequest, ::TkrzwRPC::SearchResponse
      rpc :Stream, stream(::TkrzwRPC::StreamRequest), stream(::TkrzwRPC::StreamResponse)
      rpc :Iterate, stream(::TkrzwRPC::IterateRequest), stream(::TkrzwRPC::IterateResponse)
      rpc :Replicate, ::TkrzwRPC::ReplicateRequest, stream(::TkrzwRPC::ReplicateResponse)
      rpc :ChangeMaster, ::TkrzwRPC::ChangeMasterRequest, ::TkrzwRPC::ChangeMasterResponse
    end

    Stub = Service.rpc_stub_class
  end
end
