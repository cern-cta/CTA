#include <Python.h>
#include "/usr/local/src/CASTOR2/CASTOR2/h/Cns_api.h"
#include "/usr/local/src/CASTOR2/CASTOR2/h/Cns.h"

// Function to retrieve values from the ns

static PyObject* wrap_cns_getsegattrs(PyObject* self, PyObject* args) {
  // Parse Input
  struct Cns_fileid file_uniqueid;
  memset(&file_uniqueid,'\0',sizeof(file_uniqueid));

  sprintf(file_uniqueid.server,"%s","lxs5012");
 
  if (!PyArg_ParseTuple(args,"i",&file_uniqueid.fileid)) return NULL;

  // other parameters needed from the C function

  int nbseg=0;
  struct Cns_segattrs * segattrs=NULL;
 
  // Call C function
  int  ret=Cns_getsegattrs(NULL, &file_uniqueid,&nbseg,&segattrs);
  

  // Build returned objects

  PyObject* listOfSeg=PyList_New(0);
 
  for (int i=0; i<nbseg; i++) {

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


// Function to set segment values in the ns

static PyObject* wrap_cns_setsegattrs(PyObject* self, PyObject* args) {
 
  // Parse Input

  struct Cns_segattrs repackSeg;
  memset(&repackSeg,'\0',sizeof(struct Cns_segattrs));

  struct Cns_fileid file_uniqueid;
  memset(&file_uniqueid,'\0',sizeof(file_uniqueid));
  sprintf(file_uniqueid.server,"%s","lxs5012");

  int nbseg=0;
  struct Cns_segattrs * segattrs=NULL; 
  
  int repackBlockid;
  char* repackVidString;
  

  if (!PyArg_ParseTuple(args,"iiiiiiis",&file_uniqueid.fileid,&repackSeg.compression,&repackSeg.segsize,&repackSeg.fsec,&repackSeg.copyno,&repackBlockid,&repackSeg.fseq,&repackVidString)) return NULL;

  // because of different convention used by repack to store the block id  

  repackSeg.blockid[3]=(unsigned char)((repackBlockid/0x1000000)%0x100);
  repackSeg.blockid[2]=(unsigned char)((repackBlockid/0x10000)%0x100);
  repackSeg.blockid[1]=(unsigned char)((repackBlockid/0x100)%0x100); 
  repackSeg.blockid[0]=(unsigned char)((repackBlockid/0x1)%0x100);

  sprintf(repackSeg.vid,"%s",repackVidString); // the PyArg_ParseTuple need just a pointer without memory allocated. 
   

  // Cns_getsegattrs to get the attributes that are not stored in repack db

  int  ret=Cns_getsegattrs(NULL, &file_uniqueid,&nbseg,&segattrs);
  PyObject* listOfSeg=PyList_New(0);
  if (ret<0){
    return Py_BuildValue("[i,O]", -1,listOfSeg);
  }
  

  for (int i=0; i<nbseg; i++) {
        if (repackSeg.copyno != segattrs[i].copyno) continue;


        repackSeg.s_status=segattrs[i].s_status; // not stored by repack
   // the following assumtion is WRONG ... 
   // repack it is not storing the info side then  I have to take "0" because it is  usually 0 .... To be Checked
        repackSeg.side=0; 
	strcpy(repackSeg.checksum_name,segattrs[i].checksum_name); // not stored by repack
	repackSeg.checksum=segattrs[i].checksum; //not stored by repack


	PyObject * structInfoSeg=PyList_New(0);
        int ok = PyList_Append(structInfoSeg,PyInt_FromLong((long)repackSeg.fsec));
        if (ok<0) return NULL;
        ok = PyList_Append(structInfoSeg,PyInt_FromLong((long)repackSeg.copyno));
        if (ok<0) return NULL;
	ok= PyList_Append(structInfoSeg,PyInt_FromLong((long)repackSeg.segsize));
        if (ok<0) return NULL;
	ok= PyList_Append(structInfoSeg,PyInt_FromLong((long)repackSeg.compression));
        if (ok<0) return NULL;
	//ok= PyList_Append(structInfoSeg, PyString_FromStringAndSize((const char *)repackSeg.s_status,1));
	ok= PyList_Append(structInfoSeg, PyString_FromStringAndSize("?",1));
	if (ok<0) return NULL;
	ok= PyList_Append(structInfoSeg, PyString_FromStringAndSize((const char *) repackSeg.vid,6));
        //ok= PyList_Append(structInfoSeg,PyInt_FromLong((long)repackSeg.side));
	if (ok<0) return NULL;
	ok= PyList_Append(structInfoSeg, PyString_FromStringAndSize("?",1));
        if (ok<0) return NULL;
	ok= PyList_Append(structInfoSeg,PyInt_FromLong((long)repackSeg.fseq));
	if (ok<0) return NULL;
	ok= PyList_Append(structInfoSeg, PyString_FromStringAndSize((const char *) repackSeg.blockid,4));
	//ok= PyList_Append(structInfoSeg, PyString_FromString((const char *) repackSeg.checksum_name));
        if (ok<0) return NULL;
	ok= PyList_Append(structInfoSeg, PyString_FromStringAndSize("?",1));
	//ok= PyList_Append(structInfoSeg, PyLong_FromUnsignedLong((long) repackSeg.checksum)); 
        if (ok<0) return NULL;
	ok= PyList_Append(structInfoSeg, PyString_FromStringAndSize("?",1));
	if (ok<0) return NULL;
	ok = PyList_Append(listOfSeg,structInfoSeg);
	if (ok<0) return NULL;

	// Call C function

        ret=Cns_replaceseg(file_uniqueid.server,file_uniqueid.fileid,&segattrs[i],&repackSeg);

	if (ret<0){
	  return Py_BuildValue("[i,O]", -1,listOfSeg);
	}
  
  }

  return Py_BuildValue("[i,O]", ret,listOfSeg);
}


static PyMethodDef WrapCnsMethods[] = {
  {"cnsGetSegInfo", wrap_cns_getsegattrs, METH_VARARGS, "To get information of segment from castorns."},
  {"cnsSetSegInfo", wrap_cns_setsegattrs, METH_VARARGS, "To set information of segment from castorns."},
  {NULL, NULL}
};

extern "C" void initwrapcns(){
  (void) Py_InitModule("wrapcns", WrapCnsMethods);
}
