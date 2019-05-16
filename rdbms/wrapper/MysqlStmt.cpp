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

#include "common/exception/DatabaseConstraintError.hpp"
#include "common/exception/DatabasePrimaryKeyError.hpp"
#include "common/exception/Exception.hpp"
#include "common/make_unique.hpp"
#include "common/threading/MutexLocker.hpp"

#include "rdbms/wrapper/MysqlConn.hpp"
#include "rdbms/wrapper/MysqlRset.hpp"
#include "rdbms/wrapper/MysqlStmt.hpp"

#include <mysql.h>
#include <mysqld_error.h>

#include <cstring>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <stdlib.h>
#include <string>
#include <unistd.h>

namespace cta {
namespace rdbms {
namespace wrapper {


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
MysqlStmt::MysqlStmt(
  MysqlConn &conn,
  const std::string &sql):
  StmtWrapper(sql),
  m_conn(conn),
  m_stmt(nullptr), m_param_count(0),
  m_fields_info(nullptr),
  m_nbAffectedRows(0) {

  // check connection first
  if (!m_conn.isOpen()) {
    throw exception::Exception(std::string(__FUNCTION__) + " check connection failed");
  }

  m_stmt = mysql_stmt_init(m_conn.m_mysqlConn);
  if (!m_stmt) {
    std::string msg = " out of memory when create mysql prepared statement.";
    throw exception::Exception(std::string(__FUNCTION__) + msg);
  }

  // translate sql string with named binding.
  // Mysql only accept '?' style (anonymous bindings)
  //   Ref: https://dev.mysql.com/doc/dev/mysqlsh-api-javascript/8.0/param_binding.html

  const std::string real_sql = Mysql::translate_it(sql);

  const int retcode = mysql_stmt_prepare(m_stmt, real_sql.c_str(), real_sql.length());
  if (retcode) {
    const unsigned int rc = mysql_stmt_errno(m_stmt);
    const std::string msg = std::string(" mysql errno: ") + std::to_string(rc) + " " + mysql_stmt_error(m_stmt);
    throw exception::Exception(std::string(__FUNCTION__) + msg);
  }

  // if there is any placeholder, the server will tell us

  m_param_count = mysql_stmt_param_count(m_stmt);

  // create bind parameters
  if (m_param_count>0) {
    for (int i = 0; i < m_param_count; ++i) {
      m_placeholder.push_back(nullptr);
    }

  }

  SmartMYSQL_RES result_metadata(mysql_stmt_result_metadata(m_stmt));

  if (result_metadata) {
    // initialize FieldsInfo
    m_fields_info = new Mysql::FieldsInfo(result_metadata.get());

    do_bind_results();
  }
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
MysqlStmt::~MysqlStmt() {
  try {
    close(); // Idempotent close() method
  } catch(...) {
    // Destructor does not throw
  }

  for (size_t i = 0; i < m_placeholder.size(); ++i) {
    delete m_placeholder[i];
    m_placeholder[i] = nullptr;
  }

  for (size_t i = 0; i < m_holders_results.size(); ++i) {
    delete m_holders_results[i];
    m_holders_results[i] = nullptr;
  }

  if (m_fields_info) {
    delete m_fields_info;
    m_fields_info = nullptr;
  }
}

//------------------------------------------------------------------------------
// clear
//------------------------------------------------------------------------------
void MysqlStmt::clear() {
}

//------------------------------------------------------------------------------
// close
//------------------------------------------------------------------------------
void MysqlStmt::close() {

  try {
    threading::MutexLocker locker(m_mutex);

    if (nullptr != m_stmt) {

      // release result first
      if (mysql_stmt_free_result(m_stmt)) {
        throw exception::Exception(std::string(__FUNCTION__) + " stmt free result failed.");
      }

      if (mysql_stmt_close(m_stmt)) {
        std::string msg = mysql_error(m_conn.m_mysqlConn);
        throw exception::Exception(std::string(__FUNCTION__) + msg);
      }

      m_stmt = nullptr;
    }
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// get
//------------------------------------------------------------------------------
MYSQL_STMT *MysqlStmt::get() const {
  if(nullptr == m_stmt) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed for SQL statement " +
      getSqlForException() + ": nullptr pointer");
  }
  return m_stmt;
}

//------------------------------------------------------------------------------
// bindUint64
//------------------------------------------------------------------------------
void MysqlStmt::bindUint64(const std::string &paramName, const uint64_t paramValue) {
  try {
    bindOptionalUint64(paramName, paramValue);
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// bindOptionalUint64
//------------------------------------------------------------------------------
void MysqlStmt::bindOptionalUint64(const std::string &paramName, const optional<uint64_t> &paramValue) {
  try {
    const unsigned int paramIdx = getParamIdx(paramName); // starts from 1.
    const unsigned int idx = paramIdx - 1;

    Mysql::Placeholder_Double* holder = dynamic_cast<Mysql::Placeholder_Double*>(m_placeholder[idx]);
    // if already exists, try to reuse
    if (!holder and m_placeholder[idx]) {
      throw exception::Exception(std::string(__FUNCTION__) + " can't cast from Placeholder to Placeholder_Uint64. " );
    }

    if (!holder) {
      holder = new Mysql::Placeholder_Double();
      holder->idx = idx;
      holder->length = sizeof(uint64_t);
    }

    if (paramValue) {
      holder->val = paramValue.value();
    } else {
      holder->is_null = true;
    }
    // delete m_placeholder[idx]; // remove the previous placeholder

    m_placeholder[idx] = holder;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed for SQL statement " +
      getSqlForException() + ": " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// bindDouble
//------------------------------------------------------------------------------
void MysqlStmt::bindDouble(const std::string &paramName, const double paramValue) {
  try {
    bindOptionalDouble(paramName, paramValue);
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// bindOptionalDouble
//------------------------------------------------------------------------------
void MysqlStmt::bindOptionalDouble(const std::string &paramName, const optional<double> &paramValue) {
  try {
    const unsigned int paramIdx = getParamIdx(paramName); // starts from 1.
    const unsigned int idx = paramIdx - 1;

    Mysql::Placeholder_Double* holder = dynamic_cast<Mysql::Placeholder_Double*>(m_placeholder[idx]);
    // if already exists, try to reuse
    if (!holder and m_placeholder[idx]) {
      throw exception::Exception(std::string(__FUNCTION__) + " can't cast from Placeholder to Placeholder_Double. " );
    }

    if (!holder) {
      holder = new Mysql::Placeholder_Double();
      holder->idx = idx;
      holder->length = sizeof(uint64_t);
    }

    if (paramValue) {
      holder->val = paramValue.value();
    } else {
      holder->is_null = true;
    }
    // delete m_placeholder[idx]; // remove the previous placeholder

    m_placeholder[idx] = holder;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed for SQL statement " +
      getSqlForException() + ": " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// bindString
//------------------------------------------------------------------------------
void MysqlStmt::bindString(const std::string &paramName, const std::string &paramValue) {
  try {
    bindOptionalString(paramName, paramValue);
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// bindOptionalString
//------------------------------------------------------------------------------
void MysqlStmt::bindOptionalString(const std::string &paramName, const optional<std::string> &paramValue) {
  try {
    if(paramValue && paramValue.value().empty()) {
      throw exception::Exception(std::string("Optional string parameter ") + paramName + " is an empty string. "
        " An optional string parameter should either have a non-empty string value or no value at all.");
    }

    bool is_null = not paramValue;

    const unsigned int paramIdx = getParamIdx(paramName); // starts from 1.
    const unsigned int idx = paramIdx - 1;

    Mysql::Placeholder_String* holder = dynamic_cast<Mysql::Placeholder_String*>(m_placeholder[idx]);
    // if already exists, try to reuse
    if (!holder and m_placeholder[idx]) {
      throw exception::Exception(std::string(__FUNCTION__) + " can't cast from Placeholder to Placeholder_String. " );
    }

    if (!holder) {
      const unsigned int buf_size = 4096;
      holder = new Mysql::Placeholder_String(buf_size); // hard code buffer length
      holder->idx = idx;
    }

    if (paramValue) {
      holder->length = paramValue.value().size();

      if (holder->length >= holder->get_buffer_length()) {
        throw exception::Exception(std::string(__FUNCTION__) + " length >= buffer_length");
      }

      // reset memory
      holder->reset();
      snprintf(holder->val, holder->get_buffer_length(), paramValue.value().c_str());
    } else {
      holder->length = 0;
    }

    holder->is_null= is_null;

    // delete m_placeholder[idx]; // remove the previous placeholder

    m_placeholder[idx] = holder;

  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed for SQL statement " +
      getSqlForException() + ": " + ex.getMessage().str()); 
  }
}

//------------------------------------------------------------------------------
// executeQuery
//------------------------------------------------------------------------------
std::unique_ptr<RsetWrapper> MysqlStmt::executeQuery() {
  // bind values before execute
  do_bind();

  if (mysql_stmt_execute(m_stmt)) {
    unsigned int rc = mysql_stmt_errno(m_stmt);
    std::string msg = std::string(" mysql errno: ") + std::to_string(rc) + " " + mysql_stmt_error(m_stmt);
    
    throw exception::Exception(std::string(__FUNCTION__) + msg);
  }

  if (mysql_stmt_store_result(m_stmt)) {
    unsigned int rc = mysql_stmt_errno(m_stmt);
    std::string msg = std::string(" mysql errno: ") + std::to_string(rc) + " " + mysql_stmt_error(m_stmt);
    
    throw exception::Exception(std::string(__FUNCTION__) + " store failed:" + msg);
  }

  // after execute, we need to get the results
  if (m_fields_info) {
    return cta::make_unique<MysqlRset>(*this, *m_fields_info);
  }

  return nullptr;
  // return cta::make_unique<MysqlRset>(*this);
  // throw exception::Exception(std::string(__FUNCTION__) + " Not Implemented.");
}

//------------------------------------------------------------------------------
// executeNonQuery
//------------------------------------------------------------------------------
void MysqlStmt::executeNonQuery() {
  // FIXME: deadlock found
  {
    // threading::MutexLocker connLocker(m_conn.m_mutex);

    // bind values before execute
    do_bind();

    if (mysql_stmt_execute(m_stmt)) {
      unsigned int rc = mysql_stmt_errno(m_stmt);
      std::string msg = std::string(" mysql errno: ") + std::to_string(rc) + " " + mysql_stmt_error(m_stmt);

      switch(rc) {
      case ER_DUP_ENTRY:
        throw exception::DatabasePrimaryKeyError(std::string(__FUNCTION__) + " " +  msg);
        break;
      }

      throw exception::Exception(std::string(__FUNCTION__) + " " +  msg);
    }

    if (mysql_stmt_store_result(m_stmt)) {
      unsigned int rc = mysql_stmt_errno(m_stmt);
      std::string msg = std::string(" mysql errno: ") + std::to_string(rc) + " " + mysql_stmt_error(m_stmt);
    
      throw exception::Exception(std::string(__FUNCTION__) + " store failed: " + msg);
    }

    m_nbAffectedRows = mysql_stmt_affected_rows(m_stmt);

  }
}

//------------------------------------------------------------------------------
// getNbAffectedRows
//------------------------------------------------------------------------------
uint64_t MysqlStmt::getNbAffectedRows() const {
  return m_nbAffectedRows;
}

Mysql::Placeholder* MysqlStmt::columnHolder(const std::string& colName) const {

  if (not m_fields_info->exists(colName)) {
    throw exception::Exception(std::string(__FUNCTION__) + " column does not exist: " + colName);
    return nullptr;
  }

  int idx = m_fields_info->column2idx[colName];
  return m_holders_results[idx];


}

//------------------------------------------------------------------------------
// do_bind
//------------------------------------------------------------------------------
bool MysqlStmt::do_bind() {
  // if nothing to bind, just return
  if (m_placeholder.size() == 0) {
    return true;
  }

  MYSQL_BIND* bind = new MYSQL_BIND[m_param_count];
  memset(bind, 0, sizeof(MYSQL_BIND)*m_param_count);

  for (int idx = 0; idx < m_param_count; ++idx) {
    Mysql::Placeholder* holder = m_placeholder[idx];

    if (!holder) {
      throw exception::Exception(std::string(__FUNCTION__) + " empty holder for index " + std::to_string(idx));
      return false;
    }

    switch(holder->get_buffer_type()) {
    case Mysql::Placeholder::placeholder_uint64:
      bind[idx].buffer_type = MYSQL_TYPE_LONGLONG;
      break;
    case Mysql::Placeholder::placeholder_string:
      bind[idx].buffer_type = MYSQL_TYPE_STRING;
      break;
    case Mysql::Placeholder::placeholder_double:
      bind[idx].buffer_type = MYSQL_TYPE_DOUBLE;
      break;
    default:
      throw exception::Exception(std::string(__FUNCTION__) + " Unknown buffer type in placeholder " + std::to_string(holder->get_buffer_type()));
      break;
    }

    bind[idx].buffer = holder->get_buffer();
    bind[idx].buffer_length = holder->get_buffer_length();
    bind[idx].length = holder->get_length();
    bind[idx].is_null = holder->get_is_null();
    bind[idx].is_unsigned = holder->get_is_unsigned();
  }

  // now, we can bind the value
  if (mysql_stmt_bind_param(m_stmt, bind)) {
    throw exception::Exception(std::string(__FUNCTION__) + " bind param failed.");
    return false;
  }

  delete [] bind;

  return true;
}



bool MysqlStmt::do_bind_results() {
  int num_columns = m_fields_info->column_count;

  if (num_columns==0) {
    // std::cout << "nothing to bind." << std::endl;
    throw exception::Exception(std::string(__FUNCTION__) + " nothing to bind.");
    return false;
  }

  MYSQL_BIND* bind = new MYSQL_BIND[num_columns];
  memset(bind, 0, sizeof(MYSQL_BIND)*num_columns);

  for (int i = 0; i < num_columns; ++i) {
    Mysql::Placeholder* holder = nullptr;

    // according to the type, assign values
    enum_field_types buffer_type = (enum_field_types)m_fields_info->types[i];
    // unsigned int maxsize = m_fields.maxsizes[i];


    // std::cout << __FILE__ << ":" << __LINE__ 
    //           << " TAO: "
    //           << " field " << i
    //           << " maxsize " << maxsize 
    //           << std::endl; 


    // see enum_field_types defined in mysql_com.h
    switch (buffer_type) {
    case MYSQL_TYPE_DECIMAL:
    case MYSQL_TYPE_TINY:
    case MYSQL_TYPE_SHORT:
    case MYSQL_TYPE_LONG:
    case MYSQL_TYPE_LONGLONG:
    case MYSQL_TYPE_NEWDECIMAL:
      {
        Mysql::Placeholder_Uint64* holder_ = new Mysql::Placeholder_Uint64();
        holder = holder_;
        bind[i].buffer_type = MYSQL_TYPE_LONGLONG;
      }
      break;
    case MYSQL_TYPE_VARCHAR:
    case MYSQL_TYPE_VAR_STRING:
    case MYSQL_TYPE_STRING:
      {
        const unsigned int buf_size  = 4096;
        if (buf_size < m_fields_info->maxsizes[i]) {
          throw exception::Exception(std::string(__FUNCTION__) + " buf size < m_fields_info->maxsizes[" + std::to_string(i) + "]");
        }
        Mysql::Placeholder_String* holder_ = new Mysql::Placeholder_String(buf_size);

        holder = holder_;
        bind[i].buffer_type = MYSQL_TYPE_STRING;
      }
      break;
    case MYSQL_TYPE_FLOAT:
    case MYSQL_TYPE_DOUBLE:
      {
        Mysql::Placeholder_Double* holder_ = new Mysql::Placeholder_Double();
        holder = holder_;
        bind[i].buffer_type = MYSQL_TYPE_DOUBLE;
      }
      break;
    default:
      throw exception::Exception(std::string(__FUNCTION__) + " unsupport buffer type: " + std::to_string(buffer_type));
      break;
    }

    holder->idx = i;
    bind[i].buffer = holder->get_buffer();
    bind[i].buffer_length = holder->get_buffer_length();
    bind[i].is_null = holder->get_is_null();
    bind[i].length = holder->get_length();
    bind[i].error = holder->get_error();

    m_holders_results.push_back(holder);
  }

  if (mysql_stmt_bind_result(m_stmt, bind)) {
    throw exception::Exception(std::string(__FUNCTION__) + " bind results failed.");
  }

  delete[] bind;

  return true;
}


} // Namespace wrapper
} // namespace rdbms
} // namespace cta
