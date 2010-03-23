
#include "castor/exception/Exception.hpp"
#include "castor/tape/format/ALB0100Unmarshaller.hpp"

#include <errno.h>
#include <iostream>
#include <zlib.h>  // needed for the Adler-32


//==============================================================================

const char *castor::tape::format::ALB0100Unmarshaller::headerName_V0[]={
  "VERSION_NUMBER",
  "HEADER_SIZE",
  "CHECKSUM_ALGORITHM",
  "HEADER_CHECKSUM",
  "TAPE_MARK_COUNT",
  "BLOCK_SIZE",
  "BLOCK_COUNT",
  "BLOCK_TIME_STAMP",
  "STAGER_VERSION",
  "STAGER_HOST",
  "DRIVE_NAME",
  "DRIVE_SERIAL",
  "DRIVE_FIRMWARE",
  "DRIVE_HOST",
  "VOL_DENSITY",
  "VOL_ID",
  "VOL_SERIAL",
  "DEVICE_GROUP_NAME",
  "FILE_SIZE",
  "FILE_CHECKSUM",
  "FILE_NS_HOST",
  "FILE_NS_ID",
  "FILE_PROGRESSIVE_CHECKSUM",
  "FILE_BLOCK_COUNT",
  "FILE_NAME"
};

bool  m_errorList[castor::tape::format::HEADER_ELEMENTS]; /* true = 1; (default)false = 0; */



//==============================================================================
//                      ALB0100Unmarshaller
//==============================================================================
const castor::tape::format::ALB0100Unmarshaller::Header* castor::tape::format::ALB0100Unmarshaller::unmarshallHeader(char* block, bool newFile){

  ALB0100Header header;
  uint16_t      j;

  // Re-set errosList
  for(j=0; j<HEADER_ELEMENTS; j++)m_errorList[j]=false; // Fix header set before
   
  if(newFile){// Copy all the Header's values in the structure assuming they are exact (1st file block)

    readVersion(block);
    strcpy(m_header.version_number, m_fix_header.version_number);
    m_header.header_size = m_fix_header.header_size;
    
    my_memcpy(block+alb0100::CHECKSUM_ALGORITHM_OFFSET, alb0100::CHECKSUM_ALGORITHM_LEN, m_header.checksum_algorithm );   
    my_atoi  (block+alb0100::HEADER_CHECKSUM_OFFSET   , alb0100::HEADER_CHECKSUM_LEN   , m_header.header_checksum );
    my_atoi  (block+alb0100::TAPE_MARK_COUNT_OFFSET   , alb0100::TAPE_MARK_COUNT_LEN   , m_header.tape_mark_count );
    my_atoi  (block+alb0100::BLOCK_SIZE_OFFSET        , alb0100::BLOCK_SIZE_LEN        , m_header.block_size );
    my_atoi  (block+alb0100::BLOCK_COUNT_OFFSET       , alb0100::BLOCK_COUNT_LEN       , m_header.block_count );
    my_atoi  (block+alb0100::BLOCK_TIME_STAMP_OFFSET  , alb0100::BLOCK_TIME_STAMP_LEN  , m_header.timestamp );    
    my_memcpy(block+alb0100::STAGER_VERSION_OFFSET    , alb0100:: STAGER_VERSION_LEN   , m_header.stager_version );    
    my_memcpy(block+alb0100::STAGER_HOST_OFFSET       , alb0100::STAGER_HOST_LEN       , m_header.stager_host);
    my_memcpy(block+alb0100::DRIVE_NAME_OFFSET        , alb0100::DRIVE_NAME_LEN        , m_header.drive_name );   
    my_memcpy(block+alb0100::DRIVE_SERIAL_OFFSET      , alb0100::DRIVE_SERIAL_LEN      , m_header.drive_serial );
    my_memcpy(block+alb0100::DRIVE_FIRMWARE_OFFSET    , alb0100::DRIVE_FIRMWARE_LEN    , m_header.drive_firmware);
    my_memcpy(block+alb0100::DRIVE_HOST_OFFSET        , alb0100::DRIVE_HOST_LEN        , m_header.drive_host);
    my_memcpy(block+alb0100::VOL_DENSITY_OFFSET       , alb0100::VOL_DENSITY_LEN       , m_header.vol_density);
    my_memcpy(block+alb0100::VOL_ID_OFFSET            , alb0100::VOL_ID_LEN            , m_header.vol_id);
    my_memcpy(block+alb0100::VOL_SERIAL_OFFSET        , alb0100::VOL_SERIAL_LEN        , m_header.vol_serial);
    my_memcpy(block+alb0100::DEVICE_GROUP_NAME_OFFSET , alb0100::DEVICE_GROUP_NAME_LEN , m_header.device_group_name);
    my_atoi  (block+alb0100::FILE_SIZE_OFFSET         , alb0100::FILE_SIZE_LEN         , m_header.file_size );
    my_atoi  (block+alb0100::FILE_CHECKSUM_OFFSET     , alb0100::FILE_CHECKSUM_LEN     , m_header.file_checksum );
    my_memcpy(block+alb0100::FILE_NS_HOST_OFFSET      , alb0100::FILE_NS_HOST_LEN      , m_header.file_ns_host);
    my_atoi  (block+alb0100::FILE_NS_ID_OFFSET        , alb0100::FILE_NS_ID_LEN        , m_header.file_ns_id); 
    m_header.file_progressive_checksum = adler32(0L,Z_NULL,0); // set to basic Adler-32 value (to uniform the progressive checksum mechanism)
    my_atoi  (block+alb0100::FILE_BLOCK_COUNT_OFFSET  , alb0100::FILE_BLOCK_COUNT_LEN  , m_header.file_block_count );
    my_memcpy(block+alb0100::FILE_NAME_OFFSET         , alb0100::FILE_NAME_LEN         , m_header.file_name);
    
    m_payload_size=m_header.block_size - m_header.header_size;
  }
  
  else{// Is not the first block

    // Same version of the previous block?
    if(!my_memcmp(block+VERSION_NUMBER_OFFSET, VERSION_NUMBER_LEN, m_header.version_number)){
      
      m_errorList[0]= true;// Differents IL format version! Rise an exception

      castor::exception::Exception ex(EINVAL);
      ex.getMessage() << "Version number mismatch!.";
      throw ex;
    }
    j = 0;
    m_errorList[j++]= false; // Has the correct IL version
    m_errorList[j++]= !my_atoi(block+HEADER_SIZE_OFFSET, HEADER_SIZE_LEN, header.header_size );

    m_errorList[j++]= !my_memcmp(block+alb0100::CHECKSUM_ALGORITHM_OFFSET, alb0100::CHECKSUM_ALGORITHM_LEN, m_header.checksum_algorithm );   
	 
    j++; // header checksum [3]                // upgrade immediatly anywaythe "master" variable
	 
    my_atoi(block+alb0100::TAPE_MARK_COUNT_OFFSET, alb0100::TAPE_MARK_COUNT_LEN, header.tape_mark_count );
    m_errorList[j++]= (header.tape_mark_count != m_header.tape_mark_count);// Fix value for all migration

    my_atoi(block+alb0100::BLOCK_SIZE_OFFSET, alb0100::BLOCK_SIZE_LEN, header.block_size );
    m_errorList[j++]= (header.block_size !=  m_header.block_size);

    my_atoi(block+alb0100::BLOCK_COUNT_OFFSET, alb0100::BLOCK_COUNT_LEN, header.block_count );// Upgrade value
    m_errorList[j++]= (header.block_count != m_header.block_count+1);
    if(!m_errorList[j-1]){m_header.block_count=header.block_count;}

    my_atoi(block+alb0100::BLOCK_TIME_STAMP_OFFSET, alb0100::BLOCK_TIME_STAMP_LEN, header.timestamp );// Upgrade value
    m_errorList[j++]= (header.timestamp < m_header.timestamp);
    if(!m_errorList[j-1]){m_header.timestamp=header.timestamp;}

    m_errorList[j++]= !my_memcmp(block+alb0100::STAGER_VERSION_OFFSET, alb0100::STAGER_VERSION_LEN, m_header.stager_version );    
    m_errorList[j++]= !my_memcmp(block+alb0100::STAGER_HOST_OFFSET, alb0100::STAGER_HOST_LEN, m_header.stager_host);
    m_errorList[j++]= !my_memcmp(block+alb0100::DRIVE_NAME_OFFSET, alb0100::DRIVE_NAME_LEN, m_header.drive_name );   
    m_errorList[j++]= !my_memcmp(block+alb0100::DRIVE_SERIAL_OFFSET, alb0100::DRIVE_SERIAL_LEN, m_header.drive_serial );
    m_errorList[j++]= !my_memcmp(block+alb0100::DRIVE_FIRMWARE_OFFSET, alb0100::DRIVE_FIRMWARE_LEN, m_header.drive_firmware);
    m_errorList[j++]= !my_memcmp(block+alb0100::DRIVE_HOST_OFFSET, alb0100::DRIVE_HOST_LEN, m_header.drive_host);
    m_errorList[j++]= !my_memcmp(block+alb0100::VOL_DENSITY_OFFSET, alb0100::VOL_DENSITY_LEN, m_header.vol_density);
    m_errorList[j++]= !my_memcmp(block+alb0100::VOL_ID_OFFSET, alb0100::VOL_ID_LEN, m_header.vol_id);
    m_errorList[j++]= !my_memcmp(block+alb0100::VOL_SERIAL_OFFSET, alb0100::VOL_SERIAL_LEN, m_header.vol_serial);
    m_errorList[j++]= !my_memcmp(block+alb0100::DEVICE_GROUP_NAME_OFFSET, alb0100::DEVICE_GROUP_NAME_LEN, m_header.device_group_name);

    my_atoi(block+alb0100::FILE_SIZE_OFFSET, alb0100::FILE_SIZE_LEN, header.file_size );
    m_errorList[j++]= (header.file_size != m_header.file_size);

    my_atoi(block+alb0100::FILE_CHECKSUM_OFFSET, alb0100::FILE_CHECKSUM_LEN, header.file_checksum );
    m_errorList[j++]= (header.file_checksum != m_header.file_checksum);

    m_errorList[j++]= !my_memcmp(block+alb0100::FILE_NS_HOST_OFFSET, alb0100::FILE_NS_HOST_LEN, m_header.file_ns_host);

    my_atoi(block+alb0100::FILE_NS_ID_OFFSET, alb0100::FILE_NS_ID_LEN,  header.file_ns_id); 
    m_errorList[j++]= (header.file_ns_id != m_header.file_ns_id);
	 
    j++; // progressive checksum [22]  DO NOT  upgrade (need old value)

    my_atoi(block+alb0100::FILE_BLOCK_COUNT_OFFSET, alb0100::FILE_BLOCK_COUNT_LEN, header.file_block_count );// Upgrade
    m_errorList[j++]= (header.file_block_count != m_header.file_block_count + m_payload_size);
    if(!m_errorList[j-1]){m_header.file_block_count+= m_payload_size;}

    m_errorList[j++]= !my_memcmp(block+alb0100::FILE_NAME_OFFSET, alb0100::FILE_NAME_LEN, m_header.file_name);
             
  }// end if(first block){...} else{


  // TEST file checksum
  if(m_header.file_block_count+m_payload_size >= m_header.file_size){ // is the last block of this file  
    uint32_t check; 
    check = adler32(m_header.file_progressive_checksum, (Bytef*)(block+HEADER_SIZE),m_header.file_size-m_header.file_block_count);
    m_errorList[19]|=(m_header.file_checksum != check);
    { //-----------------> visual returmm <------------------
      std::cout<<"Filename: "<<m_header.file_name<<" Checksum: "<<m_header.file_checksum<<std::endl;
    } //-----------------------------------------------------

  }

  // TEST progressive checksum
  my_atoi(block+alb0100::FILE_PROGRESSIVE_CHECKSUM_OFFSET, alb0100::FILE_PROGRESSIVE_CHECKSUM_LEN, header.file_progressive_checksum );
  m_errorList[22]=(header.file_progressive_checksum != adler32(m_header.file_progressive_checksum, (Bytef*)(block+HEADER_SIZE), m_payload_size));
  m_header.file_progressive_checksum = header.file_progressive_checksum;
  
  // TEST header checksum
  my_atoi(block+alb0100::HEADER_CHECKSUM_OFFSET, alb0100::HEADER_CHECKSUM_LEN, m_header.header_checksum );
  memset(block+alb0100::HEADER_CHECKSUM_OFFSET, ' ', alb0100::HEADER_CHECKSUM_LEN );
  header.header_checksum = adler32(adler32(0L,Z_NULL,0), (Bytef*)block, HEADER_SIZE);  
  m_errorList[3]=(header.header_checksum != m_header.header_checksum);
  
  bool result = false;
  for( j=0; j < HEADER_ELEMENTS; j++){
    result|=m_errorList[j];
  } 
  if(result){
    printHeader(m_header);//    Visual Return ------------------------
    
    castor::exception::Exception ex(EINVAL);
    ex.getMessage() << "One or more ERRORs foud during unmarshall. Check m_errorList[] for details.";
    throw ex;
  }
  
  return &m_header;
}

//==============================================================================
//                      Print Header
//==============================================================================
void castor::tape::format::ALB0100Unmarshaller::printHeader(ALB0100Header&){

  using std::cout;
  using std::endl;

  cout<<"======================================="<<endl;
  cout<<"Error  Variable name             Value "<<endl;
  cout<<"======================================="<<endl;

  cout<<" "<<m_errorList[ 0]<<"     "<<headerName_V0[0]<<"            "<<m_header.version_number <<endl;
  cout<<" "<<m_errorList[ 1]<<"     "<<headerName_V0[1]<<"               "<<m_header.header_size <<endl;
  cout<<" "<<m_errorList[ 2]<<"     "<<headerName_V0[2]<<"        "<<m_header.checksum_algorithm <<endl;
  cout<<" "<<m_errorList[ 3]<<"     "<<headerName_V0[3]<<"           "<<m_header.header_checksum <<endl;
  cout<<" "<<m_errorList[ 4]<<"     "<<headerName_V0[4]<<"           "<<m_header.tape_mark_count <<endl;
  cout<<" "<<m_errorList[ 5]<<"     "<<headerName_V0[5]<<"                "<<m_header.block_size <<endl;
  cout<<" "<<m_errorList[ 6]<<"     "<<headerName_V0[6]<<"               "<<m_header. block_count <<endl;
  cout<<" "<<m_errorList[ 7]<<"     "<<headerName_V0[7]<<"          "<<m_header.timestamp <<endl;
  cout<<" "<<m_errorList[ 8]<<"     "<<headerName_V0[8]<<"            "<<m_header.stager_version <<endl;
  cout<<" "<<m_errorList[ 9]<<"     "<<headerName_V0[9]<<"               "<<m_header.stager_host <<endl;
  cout<<" "<<m_errorList[10]<<"     "<<headerName_V0[10]<<"                "<<m_header.drive_name <<endl;
  cout<<" "<<m_errorList[11]<<"     "<<headerName_V0[11]<<"              "<<m_header.drive_serial <<endl;
  cout<<" "<<m_errorList[12]<<"     "<<headerName_V0[12]<<"            "<<m_header.drive_firmware <<endl;
  cout<<" "<<m_errorList[13]<<"     "<<headerName_V0[13]<<"                "<<m_header.drive_host <<endl;
  cout<<" "<<m_errorList[14]<<"     "<<headerName_V0[14]<<"               "<<m_header.vol_density <<endl;
  cout<<" "<<m_errorList[15]<<"     "<<headerName_V0[15]<<"                    "<<m_header.vol_id <<endl;
  cout<<" "<<m_errorList[16]<<"     "<<headerName_V0[16]<<"                "<<m_header.vol_serial <<endl;
  cout<<" "<<m_errorList[17]<<"     "<<headerName_V0[17]<<"         "<<m_header.device_group_name <<endl;
  cout<<" "<<m_errorList[18]<<"     "<<headerName_V0[18]<<"                 "<<m_header.file_size <<endl;
  cout<<" "<<m_errorList[19]<<"     "<<headerName_V0[19]<<"             "<<m_header.file_checksum <<endl;
  cout<<" "<<m_errorList[20]<<"     "<<headerName_V0[20]<<"              "<<m_header.file_ns_host <<endl;
  cout<<" "<<m_errorList[21]<<"     "<<headerName_V0[21]<<"                "<<m_header.file_ns_id <<endl;
  cout<<" "<<m_errorList[22]<<"     "<<headerName_V0[22]<<" "<<m_header.file_progressive_checksum <<endl;
  cout<<" "<<m_errorList[23]<<"     "<<headerName_V0[23]<<"          "<<m_header.file_block_count <<endl;  
  cout<<" "<<m_errorList[24]<<"     "<<headerName_V0[24]<<"                 "<<m_header.file_name <<endl<<endl<<endl;
}


