/******************************************************************************
 *                      testClobs.cpp
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 *
 * Some code to test and play around with Oracle CLOBS
 *
 * @author Giuseppe Lo Presti
 *****************************************************************************/

 
#include <iostream> 
#include <fstream> 
#include <occi.h> 
using namespace oracle::occi;
using namespace std;

/**
* The demo sample has starts from startDemo method. This method is called 
* by main. startDemo calls other methods, the supporting methods for 
* startDemo are,
* insertRows - insert the rows into the table
* dataRollBack - deletes the inserted rows
* populateClob - populates a given clob
* dumpClob - prints the clob as an integer stream
*/
/*
This code has been compiled with:
g++ -fPIC -D_LARGEFILE64_SOURCE -Wall -Wno-long-long -g -O0 -I /.../instantclient/10.2.0.3/slc4_ia32_gcc34/rdbms/public -pthread -c testClob.cpp
g++ -o testClob -pthread testClob.o -L /.../instantclient/10.2.0.3/slc4_ia32_gcc34/lib -locci -lclntsh
*/
// These will tell the type of read/write we use
#define USE_NORM 1
#define USE_CHUN 2
#define USE_BUFF 3

/* Buffer Size */
#define BUFSIZE 200;

class occiClob
{
  public: 
  string username;
  string password;
  string url;
  
  void insertRows (Connection *conn)
  throw (SQLException)
  {
    cout << "insertRows" << endl;
    Statement *stmt = conn->createStatement ("INSERT INTO clobstest(id, longdata) VALUES (:v1,:v2)");
    Clob clob(conn);
    clob.setEmpty();
    stmt->setInt(1,6666);
    stmt->setClob(2,clob);
    stmt->executeUpdate();
    stmt->setInt(1,7777);
    stmt->setClob(2,clob);
    stmt->executeUpdate();
    conn->commit();
    conn->terminateStatement (stmt);
    cout << "insertRows done" << endl;
  }
  
 
  /**
  * populating the clob uses write method;
  */
  void populateClob (Clob &clob,unsigned int way, string value)
  throw (SQLException)
  {
    if (way == USE_NORM)
    {
      cout << "Emptying the Clob" << endl;
      
      cout << "Populating the Clob using write method" << endl;
      clob.open(oracle::occi::OCCI_LOB_READWRITE);
      int len = value.size();
      unsigned int bytesWritten=clob.write(len, (unsigned char*)value.c_str(), len);
      cout <<"Bytes Written : " << bytesWritten << endl;
      clob.close();
    }
    cout << "Populating the Clob - Success" << endl;
  }
  
  /**
  * printing the clob data as integer stream
  */
  void dumpClob (Clob &clob,unsigned int way)
  throw (SQLException)
  {
    if (clob.isNull())
    {
      cout << "Clob is Null\n";
      return;
    }
    int len = clob.length();
    cout << "Length of Clob : "<< len << endl;
    if (len == 0)
      return;
    char* buf = (char*) malloc(len+1);
    buf[len] = 0;
    cout << "Dumping clob (using read ): ";
    clob.open(oracle::occi::OCCI_LOB_READONLY);
    int bytesRead=clob.read(len, (unsigned char*)buf, len+1);
    clob.close();
    cout << buf << endl;
    cout << "bytes read: " << bytesRead << endl;
    free(buf);
  }
  
  occiClob ()
  {
    /**
    * default values of username & password
    */
    username = "srmdev";
    password = "castor";
    url = "castor64";
  }
  
  void runSample ()
  throw (SQLException)
  {
    Environment *env = Environment::createEnvironment (
    Environment::DEFAULT);
    Connection *conn = env->createConnection (username, password, url);
    //insertRows (conn);
    
    // Selecting and modifying the clob column of the table
    string sqlQuery = "UPDATE clobsTest SET longdata = :v1 WHERE id = 6666";
    Statement *stmt = conn->createStatement (sqlQuery);
    Clob clob(conn);
    clob.setEmpty();
    stmt->setClob(1, clob);
    stmt->executeUpdate();
    conn->commit();
    conn->terminateStatement (stmt);
    
    sqlQuery = "SELECT id, longdata FROM clobsTest WHERE id = 6666 FOR UPDATE";
    Statement *stmt1 = conn->createStatement (sqlQuery);
    ResultSet *rset1 = stmt1->executeQuery ();
    cout << "Query :" << sqlQuery << endl;
    while (rset1->next ())
    {
      cout << "id : " << (int)rset1->getInt(1) << endl;
      Clob clob = rset1->getClob (2);
      //dumpClob(clob, USE_NORM);
      populateClob(clob, USE_NORM, "new data 123456789012345678901234"); 
    }
    stmt1->executeUpdate();
    stmt1->closeResultSet (rset1);
    conn->commit();
    conn->terminateStatement (stmt1);
    
    // Printing after updating the clob content.
    string sqlQuery = "SELECT id, longdata FROM clobsTest WHERE id = 6666";
    Statement *stmt2 = conn->createStatement (sqlQuery);
    ResultSet *rset2 = stmt2->executeQuery ();
    Clob clob;
    cout << "Query :" << sqlQuery << endl;
    while (rset2->next ())
    {
      cout << "id : " << (int)rset2->getInt(1) << endl;
      clob = rset2->getClob (2);
      dumpClob (clob, USE_NORM);
    }
    stmt2->closeResultSet (rset2);
    conn->terminateStatement (stmt2);

    //env->terminateConnection (conn);
    //Environment::terminateEnvironment (env);
  }
  
};//end of class occiClob

int main (void)
{
  try
  {
    occiClob *b = new occiClob ();
    b->runSample ();
  }
  catch (exception &e)
  {
    cout << e.what();
  }
}

/*
stmt = pOraCn->createStatement(sCLOBSQL);
if ( NULL != stmt )
{
  ResultSet* rs = NULL;
  
  rs = stmt->executeQuery();
  if ( NULL != rs )
  {
    if ( oracle::occi::ResultSet::END_OF_FETCH != rs->next() )
    {
      Clob vClob;
      
      vClob = rs->getClob(1);
      vClob.open();
      try
      {
        vClob.write(sVal.length(), (unsigned char*)sVal.c_str(), sVal.length(), 1);
      }
      catch ( ... ) 
      {
        //
      }
      vClob.close();
    }
    stmt->executeUpdate();
    stmt->closeResultSet(rs);
  }
  pOraCn->commit();
  pOraCn->terminateStatement(stmt);
}

*/
