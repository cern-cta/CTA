/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

struct st_mysql_res;
typedef st_mysql_res MYSQL_RES;

struct st_mysql_stmt;
typedef st_mysql_stmt MYSQL_STMT;

struct st_mysql_bind;
typedef st_mysql_bind MYSQL_BIND;

typedef char my_bool;

extern "C" void mysql_free_result(MYSQL_RES *result);

#include <vector>
#include <limits>
#include <map>
#include <memory>
#include <stdint.h>
#include <string.h>

namespace cta {
namespace rdbms {
namespace wrapper {

class SmartMYSQL_RES {
public:
  SmartMYSQL_RES(MYSQL_RES *res): m_res(res) {
  }

  MYSQL_RES &operator->() {
    return *m_res;
  }

  MYSQL_RES *get() {
    return m_res;
  }

  operator bool() const {
   return nullptr != m_res;
  }

  ~SmartMYSQL_RES() {
    if(nullptr != m_res) {
      mysql_free_result(m_res); 
    }
  }

private:

  MYSQL_RES *m_res;
};

/**
 * A helper class for working with MySQL.
 */

class Mysql {
  public:

  static std::string translate_it(const std::string& sql);

  /**
   * Placeholder of bind
   */
  struct Placeholder {
    unsigned int idx;
    my_bool is_null; // FIXED ME: in MySQL api, my_bool is char
    unsigned long length;
    my_bool error;

    Placeholder()
      : idx(0), is_null(false), length(0), error(0) {

    }

    virtual ~Placeholder() {

    }

    enum buffer_types {
      placeholder_uint8,
      placeholder_uint16,
      placeholder_uint32,
      placeholder_uint64,
      placeholder_string,
      placeholder_double,
    };

    virtual std::string show() = 0;

    virtual buffer_types get_buffer_type() = 0;
    virtual void* get_buffer() = 0;
    virtual unsigned long get_buffer_length() = 0;
    virtual unsigned long* get_length() { return &length; }
    virtual my_bool* get_is_null() { return &is_null; }
    virtual bool get_is_unsigned() = 0;
    virtual my_bool* get_error() { return &error; }
    // following is to access data
    virtual uint8_t get_uint8() = 0;
    virtual uint16_t get_uint16() = 0;
    virtual uint32_t get_uint32() = 0;
    virtual uint64_t get_uint64() = 0;
    virtual std::string get_string() = 0;
    virtual double get_double() = 0;
    // helper
    virtual bool reset() = 0;
  };

  struct Placeholder_Uint8: Placeholder {
    uint8_t val;

    Placeholder_Uint8()
      : Placeholder(), val(0) {

    }

    std::string show() override {
      std::stringstream ss;
      ss << "['" << idx << "' '" << is_null << "' '" << length << "' '" << val << "' '" << get_buffer_length() << "']";
      return ss.str();
    }

    buffer_types get_buffer_type() override {
      return placeholder_uint8;
    }

    void* get_buffer() override {
      return &val;
    }

    unsigned long get_buffer_length() override {
      return sizeof(uint8_t);
    }

    bool get_is_unsigned() override {
      return true;
    }

    uint8_t get_uint8() override {
      return val;
    }

    uint16_t get_uint16() override {
      return val;
    }

    uint32_t get_uint32() override {
      return val;
    }

    uint64_t get_uint64() override {
      return val;
    }

    std::string get_string() override {
      return std::to_string(val);
    }

    double get_double() override {
      return val;
    }

    bool reset() override {
      return false;
    }

  };

  struct Placeholder_Uint16: Placeholder {
    uint16_t val;

    Placeholder_Uint16()
      : Placeholder(), val(0) {

    }

    std::string show() override {
      std::stringstream ss;
      ss << "['" << idx << "' '" << is_null << "' '" << length << "' '" << val << "' '" << get_buffer_length() << "']";
      return ss.str();
    }

    buffer_types get_buffer_type() override {
      return placeholder_uint16;
    }

    void* get_buffer() override {
      return &val;
    }

    unsigned long get_buffer_length() override {
      return sizeof(uint16_t);
    }

    bool get_is_unsigned() override {
      return true;
    }

    uint8_t get_uint8() override {
      if(std::numeric_limits<uint8_t>::max() < val) {
        std::ostringstream msg;
        msg << "Cannot convert uint16 to uint8: Overflow: uint16=" << val <<
          " maxUint8=" << std::numeric_limits<uint8_t>::max();
        throw exception::Exception(msg.str());
      }
      return val;
    }

    uint16_t get_uint16() override {
      return val;
    }

    uint32_t get_uint32() override {
      return val;
    }

    uint64_t get_uint64() override {
      return val;
    }

    std::string get_string() override {
      return std::to_string(val);
    }

    double get_double() override {
      return val;
    }

    bool reset() override {
      return false;
    }

  };

  struct Placeholder_Uint32: Placeholder {
    uint32_t val;

    Placeholder_Uint32()
      : Placeholder(), val(0) {

    }

    std::string show() override {
      std::stringstream ss;
      ss << "['" << idx << "' '" << is_null << "' '" << length << "' '" << val << "' '" << get_buffer_length() << "']";
      return ss.str();
    }

    buffer_types get_buffer_type() override {
      return placeholder_uint32;
    }

    void* get_buffer() override {
      return &val;
    }

    unsigned long get_buffer_length() override {
      return sizeof(uint32_t);
    }

    bool get_is_unsigned() override {
      return true;
    }

    uint8_t get_uint8() override {
      if(std::numeric_limits<uint8_t>::max() < val) {
        std::ostringstream msg;
        msg << "Cannot convert uint32 to uint8: Overflow: uint32=" << val <<
          " maxUint8=" << std::numeric_limits<uint8_t>::max();
        throw exception::Exception(msg.str());
      }
      return val;
    }

    uint16_t get_uint16() override {
      if(std::numeric_limits<uint16_t>::max() < val) {
        std::ostringstream msg;
        msg << "Cannot convert uint32 to uint16: Overflow: uint32=" << val <<
          " maxUint16=" << std::numeric_limits<uint16_t>::max();
        throw exception::Exception(msg.str());
      }
      return val;
    }

    uint32_t get_uint32() override {
      return val;
    }

    uint64_t get_uint64() override {
      return val;
    }

    std::string get_string() override {
      return std::to_string(val);
    }

    double get_double() override {
      return val;
    }

    bool reset() override {
      return false;
    }

  };

  struct Placeholder_Uint64: Placeholder {
    uint64_t val;

    Placeholder_Uint64()
      : Placeholder(), val(0) {

    }
    
    std::string show() override {
      std::stringstream ss;
      ss << "['" << idx << "' '" << is_null << "' '" << length << "' '" << val << "' '" << get_buffer_length() << "']";
      return ss.str();
    }

    buffer_types get_buffer_type() override {
      return placeholder_uint64;
    }

    void* get_buffer() override {
      return &val;
    }

    unsigned long get_buffer_length() override {
      return sizeof(uint64_t);
    }

    bool get_is_unsigned() override {
      return true;
    }

    uint8_t get_uint8() override {
      if(std::numeric_limits<uint8_t>::max() < val) {
        std::ostringstream msg;
        msg << "Cannot convert uint64 to uint8: Overflow: uint64=" << val <<
          " maxUint8=" << std::numeric_limits<uint8_t>::max();
        throw exception::Exception(msg.str());
      }
      return val;
    }

    uint16_t get_uint16() override {
      if(std::numeric_limits<uint16_t>::max() < val) {
        std::ostringstream msg;
        msg << "Cannot convert uint64 to uint16: Overflow: uint64=" << val <<
          " maxUint16=" << std::numeric_limits<uint16_t>::max();
        throw exception::Exception(msg.str());
      }
      return val;
    }

    uint32_t get_uint32() override {
      if(std::numeric_limits<uint32_t>::max() < val) {
        std::ostringstream msg;
        msg << "Cannot convert uint64 to uint32: Overflow: uint64=" << val <<
          " maxUint32=" << std::numeric_limits<uint32_t>::max();
        throw exception::Exception(msg.str());
      }
      return val;
    }

    uint64_t get_uint64() override {
      return val;
    }

    std::string get_string() override {
      return std::to_string(val);
    }

    double get_double() override {
      return val;
    }

    bool reset() override {
      return false;
    }

  };

  struct Placeholder_String: Placeholder {
    // std::string val;
    char* val;
    unsigned int buf_sz;

    Placeholder_String(const int sz)
      : Placeholder(), val(nullptr) {
      buf_sz = sz;
      val = new char[buf_sz]();
      reset();
    }

    ~Placeholder_String() {
      delete [] val;
    }

    std::string show() override {
      std::stringstream ss;
      ss << "['" << idx << "' '" << is_null << "' '" << length << "' '" << val << "' '" << get_buffer_length() << "']";
      return ss.str();
    }


    buffer_types get_buffer_type() override {
      return placeholder_string;
    }
    
    void* get_buffer() override {
      return (void*)val;
    }

    unsigned long get_buffer_length() override {
      return buf_sz;
    }

    bool get_is_unsigned() override {
      return false;
    }

    uint8_t get_uint8() override {
      const uint64_t i = std::stoll(val);
      if(std::numeric_limits<uint8_t>::max() > i) {
        std::ostringstream msg;
        msg << "Failed to convert string to uint8: Overflow: string=" << val << " maxUint8=" <<
          std::numeric_limits<uint8_t>::max();
        throw exception::Exception(msg.str());
      }
      return i;
    }

    uint16_t get_uint16() override {
      const uint64_t i = std::stoll(val);
      if(std::numeric_limits<uint16_t>::max() > i) {
        std::ostringstream msg;
        msg << "Failed to convert string to uint16: Overflow: string=" << val << " maxUint16=" <<
          std::numeric_limits<uint16_t>::max();
        throw exception::Exception(msg.str());
      }
      return i;
    }

    uint32_t get_uint32() override {
      const uint64_t i = std::stoll(val);
      if(std::numeric_limits<uint32_t>::max() > i) {
        std::ostringstream msg;
        msg << "Failed to convert string to uint32: Overflow: string=" << val << " maxUint32=" <<
          std::numeric_limits<uint32_t>::max();
        throw exception::Exception(msg.str());
      }
      return i;
    }

    // note: allow users try to convert from string to int, 
    //       but users need to catch the exception.
    uint64_t get_uint64() override {
      return std::stoll(val);
    }

    std::string get_string() override {
      return std::string(val,*get_length());
    }

    // note: allow users try to convert from string to int,
    //       but users need to catch the exception.
    double get_double() override {
      return std::stod(val);
    }

    bool reset() override {
      memset(val, 0, buf_sz);
      
      return true;
    }

  };

  struct Placeholder_Double: Placeholder {
    double val;

    Placeholder_Double()
      : Placeholder(), val(0) {

    }

    std::string show() override {
      std::stringstream ss;
      ss << "['" << idx << "' '" << is_null << "' '" << length << "' '" << val << "' '" << get_buffer_length() << "']";
      return ss.str();
    }

    buffer_types get_buffer_type() override {
      return placeholder_double;
    }

    void* get_buffer() override {
      return &val;
    }

    unsigned long get_buffer_length() override {
      return sizeof(double);
    }

    bool get_is_unsigned() override {
      return true;
    }

    uint8_t get_uint8() override {
      throw val;
    }

    uint16_t get_uint16() override {
      throw val;
    }

    uint32_t get_uint32() override {
      throw val;
    }

    uint64_t get_uint64() override {
      return val;
    }

    std::string get_string() override {
      return std::to_string(val);
    }

    double get_double() override {
      return val;
    }

    bool reset() {
      return false;
    }

  };

  struct FieldsInfo {
    FieldsInfo(MYSQL_RES* result_metadata);

    bool exists(const std::string& column);

    int column_count;

    std::map<std::string, int> column2idx; // column name to idx
    std::vector<std::string> names; // field name
    std::vector<int> types; // field type (my_conn.h)
    std::vector<unsigned int> maxsizes; // max size for column
  };



}; // class Mysql


} // namespace wrapper
} // namespace rdbms
} // namespace cta

