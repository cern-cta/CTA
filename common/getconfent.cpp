/******************************************************************************
 *                      getconfent.cpp
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
 * C api to the castor configuration (implemented in C++)
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include "getconfent.h"
#include "stdlib.h"
#include "string.h"
#include "castor/common/CastorConfiguration.hpp"

#ifndef PATH_CONFIG
#define PATH_CONFIG "/etc/castor/castor.conf"
#endif /* PATH_CONFIG */

//------------------------------------------------------------------------------
// getconfent_fromfile
//------------------------------------------------------------------------------
extern "C" char *getconfent_fromfile(const char *filename,
                                     const char *category,
                                     const char *name,
                                     int /*flags*/) {
  try {
    // call C++ implementation
    castor::common::CastorConfiguration &config =
      castor::common::CastorConfiguration::getConfig(filename);
    return (char*)config.getConfEnt(category, name).c_str();
  } catch (castor::exception::Exception e) {
    serrno = e.code();
  }
  // nothing found
  return NULL;
}

char *getconfent(const char *category,
                 const char *name,
                 int flags) {
  // Try to get the location of the configuration file from the environment
  // variable named PATH_CONFIG.  If the enviornment variable is not set then
  // fall back to the compile-time default.
  const char *configFilename = getenv("PATH_CONFIG");
  if(NULL == configFilename) {
    configFilename = PATH_CONFIG;
  }

  return getconfent_fromfile(configFilename,category,name,flags);
}

int getconfent_parser(char **conf_val,
                      char ***result,
                      int *count)
{
  char *p,*q,*last;
  int i=0;

  /* Counting the number of strings for the array */
  if ((p = strdup(*conf_val)) == NULL) { return -1; }
  for (q = strtok_r(p," \t",&last); q != NULL; q = strtok_r(NULL," \t",&last)) i++;
  free(p);

  /* Saving the index information to pass on later */
  *count = i;
  
  /* Allocating the necessary space and parsing the string */
  if ((p = strdup(*conf_val)) == NULL) { return -1; }
  (*result) = (char **)calloc((i+1), sizeof(char *));
  if (result == NULL) { return -1; }
 
  i = 0 ;
  for (q = strtok_r(p," \t",&last);q != NULL; q = strtok_r(NULL," \t",&last)) { (*result)[i++] = strdup(q); }
  free(p);

  return 0;
}

int getconfent_multi_fromfile(const char *filename,
                              const char *category,
                              const char *name,
                              int flags,
                              char ***result,
                              int *count)
{
  char *conf_val;

  if((conf_val = getconfent_fromfile(filename,category,name,flags)) == NULL){ 
    *result = NULL;
    *count = 0;
    return 0; 
  }
 
  if ( getconfent_parser(&conf_val, result, count) == -1 ) {return -1;}

  return 0;
}



int getconfent_multi(const char *category,
                     const char *name,
                     int flags,
                     char ***result,
                     int *count)
{
  char *conf_val;
  
  if((conf_val = getconfent(category,name,flags)) == NULL) { 
    *result = NULL;
    *count = 0;
    return 0; 
  }
  
  if( getconfent_parser(&conf_val, result, count) == -1 ) {return -1;}

  return 0;
}
