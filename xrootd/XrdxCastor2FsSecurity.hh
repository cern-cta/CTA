/*******************************************************************************
 *                      XrdxCastor2FsSecurity.hh
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2012  CERN
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
 * @author Elvin Sindrilaru & Andreas Peters - CERN
 * 
 ******************************************************************************/

#ifndef __XCASTOR_FSSECURITY_HH__
#define __XCASTOR_FSSECURITY_HH__

#include "XrdAcc/XrdAccAuthorize.hh"

#define AUTHORIZE(usr, env, optype, action, pathp, edata) \
    if (usr && XrdxCastor2FS->Authorization \
    &&  !XrdxCastor2FS->Authorization->Access(usr, pathp, optype, env)) \
       {XrdxCastor2FS->Emsg(epname, edata, EACCES, action, pathp); return SFS_ERROR;}

#define AUTHORIZE2(usr,edata,opt1,act1,path1,env1,opt2,act2,path2,env2) \
       {AUTHORIZE(usr, env1, opt1, act1, path1, edata); \
        AUTHORIZE(usr, env2, opt2, act2, path2, edata); \
       }

#define OOIDENTENV(usr, env) \
    if (usr) {if (usr->name) env.Put(SEC_USER, usr->name); \
              if (usr->host) env.Put(SEC_HOST, usr->host);}

#endif // __XCASTOR_FSSECURITY_HH__
