#include <Python.h>
#include "/usr/local/src/CASTOR2/CASTOR2/h/Cns_api.h"
#include "/usr/local/src/CASTOR2/CASTOR2/h/Cns.h"

// Function to retrieve values from the ns

static PyObject* wrap_cns_getsegattrs(PyObject* self, PyObject* args) {
  // Parse Input
  struct Cns_fileid file_uniqueid;
  memset(&file_uniqueid,'\0',sizeof(file_uniqueid));

  sprintf(file_uniqueid.server,"%s","castorns");
 
  if (!PyArg_ParseTuple(args,"i",&file_uniqueid.fileid)) return NULL;

  // other parameters needed from the C function

  int nbseg=0;
  struct Cns_segattrs * segattrs=NULL;
  // printf("server %s, fileid %i",file_uniqueid.server,file_uniqueid.fileid);


  // Call C function
  int  ret=Cns_getsegattrs(NULL, &file_uniqueid,&nbseg,&segattrs);

  // Build returned objects

  PyObject* listOfSeg=PyList_New(0);
  //printf("%p",listOfSeg); 
  for (int i=0; i<nbseg; i++) {
        PyObject * structInfoSeg=PyList_New(0);
        int ok = PyList_Append(structInfoSeg,PyInt_FromLong((long)segattrs[i].fsec));
        ok = PyList_Append(structInfoSeg,PyInt_FromLong((long)segattrs[i].copyno));
        ok= PyList_Append(structInfoSeg,PyInt_FromLong((long)segattrs[i].segsize));
        ok= PyList_Append(structInfoSeg,PyInt_FromLong((long)segattrs[i].compression));
	ok= PyList_Append(structInfoSeg, PyString_FromStringAndSize((const char *) &segattrs[i].s_status,1));
	ok= PyList_Append(structInfoSeg, PyString_FromStringAndSize((const char *) segattrs[i].vid,6));
        ok= PyList_Append(structInfoSeg,PyInt_FromLong((long)segattrs[i].side));
	ok= PyList_Append(structInfoSeg,PyInt_FromLong((long)segattrs[i].fseq));
	
	ok= PyList_Append(structInfoSeg, PyString_FromStringAndSize((const char *) segattrs[i].blockid,4));
	ok= PyList_Append(structInfoSeg, PyString_FromString((const char *) segattrs[i].checksum_name));
	ok= PyList_Append(structInfoSeg, PyLong_FromUnsignedLong((long)segattrs[i].checksum)); 
	ok = PyList_Append(listOfSeg,structInfoSeg);
	
	//printf("");


	/*	printf("\n OLD segment FROM get segment \n");
        printf("copyno %i fsec %i  segsize %i compression %i status %c ",segattrs[i].copyno,segattrs[i].fsec,segattrs[i].segsize,segattrs[i].compression,segattrs[i].s_status);
        printf("vid %s ",segattrs[i].vid);
        printf("side %i fseq %i ",segattrs[i].side,segattrs[i].fseq);
	printf("blockid %s ",segattrs[i].blockid);
	printf("checksum_name %s ",segattrs[i].checksum_name);
        printf("checksum %i ",segattrs[i].checksum);*/

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
  sprintf(file_uniqueid.server,"%s","castorns");

  int nbseg=0;
  struct Cns_segattrs * segattrs=NULL; 
  
  int repackBlockid;
  char* repackVidString;
  

  if (!PyArg_ParseTuple(args,"iiiiiiis",&file_uniqueid.fileid,&repackSeg.compression,&repackSeg.segsize,&repackSeg.fsec,&repackSeg.copyno,&repackBlockid,&repackSeg.fseq,&repackVidString)) return NULL;

  
  repackSeg.blockid[3]=(unsigned char)((repackBlockid/0x1000000)%0x100);
  repackSeg.blockid[2]=(unsigned char)((repackBlockid/0x10000)%0x100);
  repackSeg.blockid[1]=(unsigned char)((repackBlockid/0x100)%0x100); 
  repackSeg.blockid[0]=(unsigned char)((repackBlockid/0x1)%0x100);


  //printf("repack block id first value %d",repackBlockid);

  /* printf("   Values from repack db (? mark for the not provided information):\n");


    printf("    [[%d,",repackSeg.fsec);
    printf(" %d,",repackSeg.copyno);
    printf(" %d,",repackSeg.segsize);
    printf(" %d,",repackSeg.compression);
    printf(" ? , \'%s\',",repackVidString);
    printf(" ? ,  %d,",repackSeg.fseq);
    printf(" \'%c%c%c%c\', ? , ? ]]\n",repackSeg.blockid);
  
      u_signed64 total_blockid = 0;   
      total_blockid =                 repackSeg.blockid[3] * 0x1000000;
      total_blockid = total_blockid | repackSeg.blockid[2] * 0x10000;
      total_blockid = total_blockid | repackSeg.blockid[1] * 0x100;
      total_blockid = total_blockid | repackSeg.blockid[0];*/
 
      // printf("valore ricostruito %d",total_blockid);

  sprintf(repackSeg.vid,"%s",repackVidString);

  /*printf("\n JUST just after parsing \n");
  printf("file %i ",file_uniqueid.fileid);
  printf("copyno %i",repackSeg.copyno);
  printf ("fsec %i " ,repackSeg.fsec);
  printf("segsize %i",repackSeg.segsize);
  printf("compression %i",repackSeg.compression); 
  printf("vid %s ",repackSeg.vid);
  printf("fseq %i ",repackSeg.fseq);
  printf("blockid prima %i",repackBlockid);
  printf(" e dopo %s ",repackSeg.blockid);*/




  // other parameters needed from the C function (information that don't change after repack and old segment to be replaced

  int  ret=Cns_getsegattrs(NULL, &file_uniqueid,&nbseg,&segattrs);
 
   PyObject* listOfSeg=PyList_New(0);

   for (int i=0; i<nbseg; i++) {
        repackSeg.s_status=segattrs[i].s_status;

        repackSeg.side=1; // this line is WRONG ... but repack it is not storing the info side then  I have to take "1" because it is  usually 1 .... To be Checked

	strcpy(repackSeg.checksum_name,segattrs[i].checksum_name);
	repackSeg.checksum=segattrs[i].checksum; 

	// Call C function
	


	/*  printf("\n REPLACE old segment\n");
        printf("copyno %i",segattrs[i].copyno);
        printf(" fsec %i ",segattrs[i].fsec);
        printf(" segsize %i ",segattrs[i].segsize);
        printf(" compression %i ",segattrs[i].compression);
        printf(" status %c ",segattrs[i].s_status);
        printf("vid %s ",segattrs[i].vid);
        printf("side %i" ,segattrs[i].side);
        printf("fseq %i ",segattrs[i].fseq);
	printf("blockid %s ",segattrs[i].blockid);
	printf("checksum_name %s ",segattrs[i].checksum_name);
        printf("checksum %i ",segattrs[i].checksum);

	*/

       
  
 //printf("%p",listOfSeg); 

        PyObject * structInfoSeg=PyList_New(0);
        int ok = PyList_Append(structInfoSeg,PyInt_FromLong((long)repackSeg.fsec));
        ok = PyList_Append(structInfoSeg,PyInt_FromLong((long)repackSeg.copyno));
        ok= PyList_Append(structInfoSeg,PyInt_FromLong((long)repackSeg.segsize));
        ok= PyList_Append(structInfoSeg,PyInt_FromLong((long)repackSeg.compression));
        //ok= PyList_Append(structInfoSeg, PyString_FromStringAndSize((const char *)repackSeg.s_status,1));
	ok= PyList_Append(structInfoSeg, PyString_FromStringAndSize("?",1));
	ok= PyList_Append(structInfoSeg, PyString_FromStringAndSize((const char *) repackSeg.vid,6));
        //ok= PyList_Append(structInfoSeg,PyInt_FromLong((long)repackSeg.side));
	ok= PyList_Append(structInfoSeg, PyString_FromStringAndSize("?",1));
        ok= PyList_Append(structInfoSeg,PyInt_FromLong((long)repackSeg.fseq));
	ok= PyList_Append(structInfoSeg, PyString_FromStringAndSize((const char *) repackSeg.blockid,4));
	//ok= PyList_Append(structInfoSeg, PyString_FromString((const char *) repackSeg.checksum_name));
        ok= PyList_Append(structInfoSeg, PyString_FromStringAndSize("?",1));
	//ok= PyList_Append(structInfoSeg, PyLong_FromUnsignedLong((long) repackSeg.checksum)); 
        ok= PyList_Append(structInfoSeg, PyString_FromStringAndSize("?",1));

	ok = PyList_Append(listOfSeg,structInfoSeg);
        

	/*  printf("\n new segment\n");
       
        printf("copyno %i",repackSeg.copyno);
        printf(" fsec %i ",repackSeg.fsec);
        printf(" segsize %i ",repackSeg.segsize);
        printf(" compression %i ",repackSeg.compression);
        printf(" status %c ",repackSeg.s_status);
        printf("vid %s ",repackSeg.vid);
        printf("side %i" ,repackSeg.side);
        printf("fseq %i ",repackSeg.fseq);
	printf("blockid %s ",repackSeg.blockid);
	printf("checksum_name %s ",repackSeg.checksum_name);
        printf("checksum %i ",repackSeg.checksum);
        */

	if (repackSeg.copyno != segattrs[i].copyno) continue;
        // IMPORTANT  ret=Cns_replaceseg(file_uniqueid.server,file_uniqueid.fileid,&segattrs[i],&repackSeg);
	
     
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
