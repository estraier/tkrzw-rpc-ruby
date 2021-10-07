#! /usr/bin/ruby -I. -w
# -*- coding: utf-8 -*-

require 'fileutils'
require 'test/unit'
require 'tkrzw_rpc'
require 'tmpdir'

include TkrzwRPC

class TkrzwTest < Test::Unit::TestCase

  # Prepares resources.
  def setup
    @tmp_dir = Dir.mktmpdir("tkrzw-")
  end

  # Cleanups resources.
  def teardown
    FileUtils.rm_rf(@tmp_dir)
  end

  # Makes a temporary path.
  def _make_tmp_path(name)
    File.join(@tmp_dir, name)
  end

  # Status tests.
  def test_status
    status = Status.new
    assert_equal(Status::SUCCESS, status.code)
    assert_equal(Status::SUCCESS, status)
    assert_not_equal(Status::UNKNOWN_ERROR, status)
    assert_equal("", status.message)
    assert_true(status.ok?)
    status.set(Status::NOT_FOUND_ERROR, "foobar")
    assert_equal("NOT_FOUND_ERROR: foobar", status.to_s)
    assert_equal(Status::NOT_FOUND_ERROR, status.to_i)
    assert_false(status.ok?)
    s2 = Status.new(Status::NOT_IMPLEMENTED_ERROR, "void")
    status.join(s2)
    assert_equal("NOT_FOUND_ERROR: foobar", status.to_s)
    status.set(Status::SUCCESS, "OK")
    status.join(s2)
    assert_equal("NOT_IMPLEMENTED_ERROR: void", status.to_s)
    expt = assert_raises StatusException do
      status.or_die
    end
    assert_true(expt.is_a?(StatusException))
    assert_equal("NOT_IMPLEMENTED_ERROR: void", expt.message)
    assert_equal(status, expt.status)
    assert_equal("SUCCESS", Status.code_name(Status::SUCCESS))
    assert_equal("INFEASIBLE_ERROR", Status.code_name(Status::INFEASIBLE_ERROR))
  end

  # Basic tests.
  def test_basic
    dbm = RemoteDBM.new
    assert_equal(0, dbm.to_s.index("RemoteDBM"))
    assert_equal(0, dbm.inspect.index("#<TkrzwRPC::RemoteDBM"))
    assert_equal(-1, dbm.to_i)
    assert_equal(Status::SUCCESS, dbm.connect("localhost:1978"))
    assert_equal(Status::SUCCESS, dbm.set_dbm_index(-1))
    attrs = dbm.inspect_details
    assert_true(attrs["version"].length > 3)
    assert_true(attrs["num_dbms"].length > 0)
    assert_equal(Status::SUCCESS, dbm.set_dbm_index(0))
    attrs = dbm.inspect_details
    assert_true(attrs["class"].length > 3)
    assert_true(attrs["num_records"].length > 0)
    assert_equal(Status::SUCCESS, dbm.set_encoding("UTF-8"))
    status = Status.new(Status::UNKNOWN_ERROR)
    assert_equal("hello", dbm.echo("hello", status))
    assert_equal(Status::SUCCESS, status)
    assert_equal(Status::SUCCESS, dbm.clear)
    assert_equal(Status::SUCCESS, dbm.set("one", "ichi", false))
    assert_equal(Status::DUPLICATION_ERROR, dbm.set("one", "first", false))
    assert_equal(Status::SUCCESS, dbm.set("one", "first", true))
    assert_equal("first", dbm.get("one"))
    assert_equal(nil, dbm.get("two", status))
    assert_equal(Status::NOT_FOUND_ERROR, status)
    assert_equal(Status::SUCCESS, dbm.append("two", "second", ":"))
    assert_equal("second", dbm.get("two"))
    assert_equal(Status::SUCCESS, dbm.append("two", "second", ":"))
    assert_equal("second:second", dbm.get("two"))
    assert_equal(Status::SUCCESS, dbm.remove("two"));
    assert_equal(Status::NOT_FOUND_ERROR, dbm.remove("two"));
    assert_equal(Status::SUCCESS, dbm.set("日本", "東京"))
    assert_equal("東京", dbm.get("日本"))
    assert_equal(Status::SUCCESS, dbm.remove("日本"))
    assert_equal(Status::SUCCESS, dbm.set_multi(true, one: "FIRST", two: "SECOND"))
    records = dbm.get_multi("one", "two", "three")
    assert_equal(2, records.length)
    assert_equal("FIRST", records["one"])
    assert_equal("SECOND", records["two"])
    assert_equal(Status::SUCCESS, dbm.remove_multi("one", "two"))
    assert_equal(Status::NOT_FOUND_ERROR, dbm.remove_multi("one"))
    assert_equal(Status::SUCCESS, dbm.append_multi(":", one: "first", two: "second"))
    assert_equal(Status::SUCCESS, dbm.append_multi(":", one: "1", two: "2"))
    records = dbm.get_multi("one", "two")
    assert_equal("first:1", records["one"])
    assert_equal("second:2", records["two"])
    assert_equal(Status::SUCCESS, dbm.compare_exchange("one", "first:1", nil))
    assert_equal(nil, dbm.get("one"))
    assert_equal(Status::SUCCESS, dbm.compare_exchange("one", nil, "hello"))
    assert_equal("hello", dbm.get("one"))
    assert_equal(Status::INFEASIBLE_ERROR, dbm.compare_exchange("one", nil, "hello"))
    assert_equal(Status::SUCCESS, dbm.compare_exchange_multi(
                   [["one", "hello"], ["two", "second:2"]], [["one", nil], ["two", nil]]))
    assert_equal(nil, dbm.get("one"))
    assert_equal(nil, dbm.get("two"))
    assert_equal(Status::SUCCESS, dbm.compare_exchange_multi(    
                   [["one", nil], ["two", nil]], [["one", "first"], ["two", "second"]]))
    assert_equal("first", dbm.get("one"))
    assert_equal("second", dbm.get("two"))
    status = Status.new(Status::UNKNOWN_ERROR)
    assert_equal(105, dbm.increment("num", 5, 100, status))
    assert_equal(Status::SUCCESS, status)
    assert_equal(110, dbm.increment("num", 5))
    assert_equal(3, dbm.count)
    assert_equal(3, dbm.to_i)
    assert_true(dbm.file_size >= 0)
    assert_equal(Status::SUCCESS, dbm.rebuild)
    assert_false(dbm.should_be_rebuilt?)
    assert_equal(Status::SUCCESS, dbm.synchronize(false))
    (0...10).each { |i|
      assert_equal(Status::SUCCESS, dbm.set(i, i))
    }
    keys = dbm.search("regex", "[23]$", 5)
    assert_equal(2, keys.length)
    assert_true(keys.include?("2"))
    assert_true(keys.include?("3"))
    assert_equal(Status::SUCCESS, dbm.clear)
    assert_equal("tokyo", dbm["japan"] = "tokyo")
    assert_equal("tokyo", dbm["japan"])
    assert_equal("", dbm.delete("japan"))
    (1..10).each { |i|
      dbm[i] = i * i
      assert_equal((i * i).to_s, dbm[i])
    }
    count = 0
    dbm.each { |key, value|
      assert_equal(key.to_i ** 2, value.to_i)
      count += 1
    }
    assert_equal(dbm.count, count)
    assert_equal(Status::SUCCESS, dbm.disconnect)
  end

  # Iterator tests.
  def test_iterator
    dbm = RemoteDBM.new
    assert_equal(Status::SUCCESS, dbm.connect("localhost:1978"))
    assert_equal(Status::SUCCESS, dbm.clear)
    (0...10).each { |i|
      assert_equal(Status::SUCCESS, dbm.set(i, i * i))
    }
    iter = dbm.make_iterator
    assert_equal(0, iter.inspect.index("#<TkrzwRPC::Iterator"))
    assert_equal(0, iter.to_s.index("Iterator"))
    assert_equal(Status::SUCCESS, iter.first)
    count = 0
    while true
      status = Status.new(Status::UNKNOWN_ERROR)
      record = iter.get(status)
      if record
        assert_equal(Status::SUCCESS, status)
      else
        assert_equal(Status::NOT_FOUND_ERROR, status)
        break
      end
      assert_equal(record[0].to_i ** 2, record[1].to_i)
      status.set(Status::UNKNOWN_ERROR)
      assert_equal(record[0], iter.get_key(status))
      assert_equal(Status::SUCCESS, status)
      assert_equal(record[1], iter.get_value(status))
      assert_equal(Status::SUCCESS, status)
      assert_equal(Status::SUCCESS, iter.next)
      count += 1
    end
    assert_equal(dbm.count, count)
    (0...10).each { |i|
      assert_equal(Status::SUCCESS, iter.jump(i))
      record = iter.get(status)
      assert_equal(i, record[0].to_i)
      assert_equal(i * i, record[1].to_i)
    }
    status = iter.last
    if status == Status::SUCCESS
      count = 0
      while true
        status = Status.new(Status::SUCCESS)
        record = iter.get(status)
        if record
          assert_equal(Status::SUCCESS, status)
        else
          assert_equal(Status::NOT_FOUND_ERROR, status)
          break
        end
        assert_equal(record[0].to_i ** 2, record[1].to_i)
        assert_equal(Status::SUCCESS, iter.previous)
        count += 1
      end
      assert_equal(dbm.count, count)
      assert_equal(Status::SUCCESS, iter.jump_lower("0"))
      assert_equal(nil, iter.get_key(status))
      assert_equal(Status::NOT_FOUND_ERROR, status)
      assert_equal(Status::SUCCESS, iter.jump_lower("0", true))
      assert_equal("0", iter.get_key(status))
      assert_equal(Status::SUCCESS, status)
      assert_equal(Status::SUCCESS, iter.next)
      assert_equal("1", iter.get_key)
      assert_equal(Status::SUCCESS, iter.jump_upper("9"))
      assert_equal(nil, iter.get_key(status))
      assert_equal(Status::NOT_FOUND_ERROR, status)
      assert_equal(Status::SUCCESS, iter.jump_lower("9", true))
      assert_equal("9", iter.get_key(status))
      assert_equal(Status::SUCCESS, status)
      assert_equal(Status::SUCCESS, iter.previous)
      assert_equal("8", iter.get_key)
      assert_equal(Status::SUCCESS, iter.set("eight"))
      assert_equal("eight", iter.get_value)
      assert_equal(Status::SUCCESS, iter.remove)
      assert_equal("9", iter.get_key)
      assert_equal(Status::SUCCESS, iter.remove)
      assert_equal(Status::NOT_FOUND_ERROR, iter.remove)
      assert_equal(8, dbm.count)
    else
      assert_equal(Status::NOT_IMPLEMENTED_ERROR, status)
    end
    iter.destruct    
    assert_equal(Status::SUCCESS, dbm.disconnect)
    dbm.destruct
  end

  # Thread tests
  def test_thread
    dbm = RemoteDBM.new
    assert_equal(Status::SUCCESS, dbm.connect("localhost:1978"))
    assert_equal(Status::SUCCESS, dbm.clear)
    is_ordered = ["TreeDBM", "SkipDBM", "BabyDBM", "StdTreeDBM"].include?(
                   dbm.inspect_details["class"])
    rnd_state = Random.new
    num_records = 1000
    num_threads = 5
    records = {}
    tasks = []
    (0...num_threads).each do |param_thid|
      th = Thread.new(param_thid) do |thid|
        (0...num_records).each do |i|
          key_num = rnd_state.rand(num_records)
          key_num = key_num - key_num % num_threads + thid
          key = key_num.to_s
          value = (key_num * key_num).to_s
          if rnd_state.rand(num_records) == 0
            assert_equal(Status::SUCCESS, dbm.rebuild)
          elsif rnd_state.rand(10) == 0
            iter = dbm.make_iterator
            iter.jump(key)
            status = Status.new
            record = iter.get(status)
            if status == Status::SUCCESS
              assert_equal(2, record.size)
              if not is_ordered
                assert_equal(key, record[0])
                assert_equal(value, record[1])
              end
              status = iter.next
              assert_true(status == Status::SUCCESS || status == Status::NOT_FOUND_ERROR)
            end
            iter.destruct
          elsif rnd_state.rand(4) == 0
            status = Status.new
            rec_value = dbm.get(key, status)
            if status == Status::SUCCESS
              assert_equal(value, rec_value)
            else
              assert_equal(Status::NOT_FOUND_ERROR, status)
            end
          elsif rnd_state.rand(4) == 0
            status = dbm.remove(key)
            if status == Status::SUCCESS
              records.delete(key)
            else
              assert_equal(Status::NOT_FOUND_ERROR, status)
            end
          else
            overwrite = rnd_state.rand(2) == 0
            status = dbm.set(key, value, overwrite)
            if status == Status::SUCCESS
              records[key] = value
            else
              assert_equal(Status::DUPLICATION_ERROR, status)
            end
          end
          if rnd_state.rand(10) == 0
            Thread.pass
          end
        end
      end
      tasks.push(th)
    end
    tasks.each do |th|
      th.join
    end
    iter_records = {}
    dbm.each do |key, value|
      iter_records[key] = value
    end
    assert_equal(records, iter_records)
    assert_equal(Status::SUCCESS, dbm.disconnect)
    dbm.destruct
  end
end


# END OF FILE
