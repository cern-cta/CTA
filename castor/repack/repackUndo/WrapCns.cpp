#include <Python.h>
#include "/usr/local/src/CASTOR2/CASTOR2/h/Cns_api.h"
#include "/usr/local/src/CASTOR2/CASTOR2/h/Cns.h"
#include "/usr/local/src/CASTOR2/CASTOR2/castor/repack/DatabaseHelper.hpp"
#include "/usr/local/src/CASTOR2/CASTOR2/castor/repack/RepackRequest.hpp"
#include "/usr/local/src/CASTOR2/CASTOR2/castor/repack/RepackSubRequest.hpp"
#include "/usr/local/src/CASTOR2/CASTOR2/castor/repack/RepackSegment.hpp"

// Function to retrieve values from the ns





static PyObject* wrap_cns_getsegattrs(PyObject* self, PyObject* args) {
  // Parse Input
  struct Cns_fileid file_uniqueid;
  memset(&file_uniqueid,'\0',sizeof(file_uniqueid));

  sprintf(file_uniqueid.server,"%s","lxs5012");
  int copyNum; 

  if (!PyArg_ParseTuple(args,"ii",&file_uniqueid.fileid,&copyNum)) return NULL;

  // other parameters needed from the C function

  int nbseg=0;
  struct Cns_segattrs * segattrs=NULL;
 
  // Call C function
  int  ret=Cns_getsegattrs(NULL, &file_uniqueid,&nbseg,&segattrs);
  

  // Build returned objects

  PyObject* listOfSeg=PyList_New(0);
 
  for (int i=0; i<nbseg; i++) {
        
        if (copyNum != segattrs[i].copyno) continue;
    
        PyObject * structInfoSeg=PyList_New(0);
        int ok = PyList_Append(structInfoSeg,PyInt_FromLong((long)segattrs[i].fsec));
        if (ok<0) return NULL;
        ok = PyList_Append(structInfoSeg,PyInt_FromLong((long)segattrs[i].copyno));
	if (ok<0) return NULL;
        ok= PyList_Append(structInfoSeg,PyInt_FromLong((long)segattrs[i].segsize));
        if (ok<0) return NULL;
	ok= PyList_Append(structInfoSeg,PyInt_FromLong((long)segattrs[i].compression));
	if (ok<0) return NULL;
	ok= PyList_Append(structInfoSeg, PyString_FromStringAndSize((const char *) &segattrs[i].s_status,1));
	if (ok<0) return NULL;
	ok= PyList_Append(structInfoSeg, PyString_FromStringAndSize((const char *) segattrs[i].vid,6));
        if (ok<0) return NULL;
	ok= PyList_Append(structInfoSeg,PyInt_FromLong((long)segattrs[i].side));
	if (ok<0) return NULL;
	ok= PyList_Append(structInfoSeg,PyInt_FromLong((long)segattrs[i].fseq));
	if (ok<0) return NULL;
	ok= PyList_Append(structInfoSeg, PyString_FromStringAndSize((const char *) segattrs[i].blockid,4));
	if (ok<0) return NULL;
	ok= PyList_Append(structInfoSeg, PyString_FromString((const char *) segattrs[i].checksum_name));
	if (ok<0) return NULL;
	ok= PyList_Append(structInfoSeg, PyLong_FromUnsignedLong((long)segattrs[i].checksum)); 
	if (ok<0) return NULL;
	ok = PyList_Append(listOfSeg,structInfoSeg);
	if (ok<0) return NULL;
  }

  return Py_BuildValue("[i,O]", ret,listOfSeg);
}


// Function to set segment values in the ns for  all the segment in a specific tape 

static PyObject* wrap_cns_setsegattrs(PyObject* self, PyObject* args) {

  castor::BaseObject::initLog("Repack", castor::SVC_STDMSG);


  // Parse Input

  int nbseg=0;
  struct Cns_segattrs * segattrs=NULL; 
  struct Cns_segattrs repackSeg;
  memset(&repackSeg,'\0',sizeof(struct Cns_segattrs));

  struct Cns_fileid file_uniqueid;
  memset(&file_uniqueid,'\0',sizeof(file_uniqueid));
  sprintf(file_uniqueid.server,"%s","lxs5012");
  unsigned  int repackBlockid;
  char* repackVidString;
  int i,ret,ok;
  
  
  if (!PyArg_ParseTuple(args,"s",&repackVidString)) return Py_BuildValue("[i,s]", ret,"Vid String not Valid") ;
 
  castor::repack::DatabaseHelper dbHelper;

  castor::repack::RepackRequest* repackReqInfo= dbHelper.getLastTapeInformation(std::string(repackVidString));

  //  std::vector<castor::repack::RepackSegment*>::iterator segiter = repackSubReqInfo->segment().begin();
  // std::vector<castor::repack::RepackSegment*>::iterator segend = repackSubReqInfo->segment().end();

  std::vector<castor::repack::RepackSegment*>::iterator segiter = repackReqInfo->subRequest().at(0)->segment().begin();
  std::vector<castor::repack::RepackSegment*>::iterator segend = repackReqInfo->subRequest().at(0)->segment().end();
  PyObject* listOfFileResult=PyList_New(0);

  while ( segiter != segend ){
       repackSeg.fsec=(*segiter)->filesec(); 
       repackSeg.copyno=(*segiter)->copyno();
       repackSeg.segsize=(*segiter)->segsize();
       repackSeg.compression=(*segiter)->compression();
       repackSeg.fseq=(*segiter)->fileseq();
       
       sprintf(repackSeg.vid,"%s",repackVidString);
       
       unsigned int repackBlockid=(*segiter)->blockid();
       repackSeg.blockid[3]=(unsigned char)((repackBlockid/0x1000000)%0x100);
       repackSeg.blockid[2]=(unsigned char)((repackBlockid/0x10000)%0x100);
       repackSeg.blockid[1]=(unsigned char)((repackBlockid/0x100)%0x100); 
       repackSeg.blockid[0]=(unsigned char)((repackBlockid/0x1)%0x100);

       // information not stored by Repack

       file_uniqueid.fileid=(*segiter)->fileid();
       ret=Cns_getsegattrs(NULL, &file_uniqueid,&nbseg,&segattrs);
       if (ret<0){
	 ok = PyList_Append(listOfFileResult,Py_BuildValue("[i,i]", -1,file_uniqueid.fileid));
         if (ok<0)return NULL;
	 segiter++;
	 continue; 
       }

       for (i=0; i<nbseg; i++) {
	 if (repackSeg.copyno != segattrs[i].copyno) continue;

         repackSeg.s_status=segattrs[i].s_status; // not stored by repack
   // the following assumtion is WRONG ... 
   // repack it is not storing the info side then  I have to take "0" because it is  usually 0 .... To be Checked
         repackSeg.side=0; 
	 strcpy(repackSeg.checksum_name,segattrs[i].checksum_name); // not stored by repack
	 repackSeg.checksum=segattrs[i].checksum; //not stored by repack
       }


	// Call C function
       
       i--;
       ret=Cns_replaceseg(file_uniqueid.server,file_uniqueid.fileid,&segattrs[i],&repackSeg);
       
       
       if (ret<0){
	 ok = PyList_Append(listOfFileResult,Py_BuildValue("[i,i]", -1,file_uniqueid.fileid));
         segiter++;
         continue;
       }
       else{
	 ok = PyList_Append(listOfFileResult,Py_BuildValue("[i,i]", 0,file_uniqueid.fileid));
         segiter++;
         continue;
       }

 }
 return Py_BuildValue("[O]", listOfFileResult);

}





static PyMethodDef WrapCnsMethods[] = {
  {"cnsGetSegInfo", wrap_cns_getsegattrs, METH_VARARGS, "To get information of segment from castorns."},
  {"cnsSetSegInfo", wrap_cns_setsegattrs, METH_VARARGS, "To set information of segment from castorns."},
  {NULL, NULL}
};

extern "C" void initwrapcns(){
  (void) Py_InitModule("wrapcns", WrapCnsMethods);
}
