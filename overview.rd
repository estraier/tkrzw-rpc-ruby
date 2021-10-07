= Ruby client library of Tkrzw-RPC

Tkrzw-RPC: RPC interface of Tkrzw

== Introduction

The core package of Tkrzw-RPC provides a server program which manages databases of Tkrzw.  This package provides a Ruby client library to access the service via gRPC protocol.  Tkrzw is a library to mange key-value storages in various algorithms.  With Tkrzw, the application can handle database files efficiently in process without any network overhead.  However, it means that multiple processes cannot open the same database file simultaneously.  Tkrzw-RPC solves the issue by using a server program which manages database files and allowing other processes access the contents via RPC.

The class "RemoteDBM" has a similar API to the local DBM API, which represents an associative array aka a Hash in Ruby.  Read the homepage https://dbmx.net/tkrzw-rpc/ for details.

The module file is "tkrzw_rpc", which defines the module "TkrzwRPC".

  require 'tkrzw_rpc'

An instance of the class "RemoteDBM" is used in order to handle a database.  You can store, delete, and retrieve records with the instance.  The result status of each operation is represented by an object of the class "Status".  Iterator to access access each record is implemented by the class "Iterator".

== Installation

This package is independent of the core library of Tkrzw.  You don't have to install the core library.  Meanwhile, you have to install the library of gRPC for Ruby as described in the official document.  Ruby 2.7 or later is required to use this package.

Enter the directory of the extracted package then perform installation.  If your system has the another command except for the "ruby" command, edit the Makefile beforehand.

  make
  sudo make install

To perform the integration tests, run these command on two respective terminals.

  tkrzw_server
  make check

== Example

Before running these examples, you have to run a database server by the following command.  It runs the server at the port 1978 on the local machine.

  tkrzw_server 

The following code is a typical example to use a database.  A RemoteDBM object can be used like a Hash object.  The "each" iterator is useful to access each record in the database.

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
   
  # Traverses records.
  dbm.each do |key, value|
    p key + ": " + value
  end
   
  # Closes and the connection and releases the resources.
  dbm.disconnect
  dbm.destruct

The following code is a more complex example.  You should use "ensure" clauses to destruct instances of DBM and Iterator, in order to release unused resources.  Even if the connection is not closed, the destructor closes it implicitly.  The method "or_die" throws an exception on failure so it is useful for checking errors.

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
