
#include "castor/tape/format/ALB0100Marshaller.hpp"

#include <errno.h>
#include <iostream>
#include <zlib.h>  // needed for the Adler-32
#include <typeinfo>


//----------------------------------------------------------------------------
// destructor
//----------------------------------------------------------------------------
castor::tape::format::ALB0100Marshaller::~ALB0100Marshaller() {
  // Do nothing
}


//----------------------------------------------------------------------------
// startMigration
//----------------------------------------------------------------------------
void castor::tape::format::ALB0100Marshaller::startMigration(Header &data)
throw(castor::exception::Exception) {
    
  char       str[31];
  MigHeader *mig;

  try{
    mig = dynamic_cast<MigHeader*>(&data);
  }catch(std::bad_cast e){
    castor::exception::Exception ex(EINVAL);
    ex.getMessage()<<e.what() << "Failed to dynamic cast header to MigHeader";
    throw ex;
  }

  if(mig == NULL) {
    castor::exception::Exception ex(EINVAL);
    ex.getMessage() << "Failed to dynamic cast header to MigHeader";
    throw ex;
  }

  memset(m_headerStamp, 'x', HEADER_SIZE);  // for visual effect!!!! <------------------------------
    
 
  m_block_count = mig->block_count;  // save the block counter in a variable, NOT in the header!    
  parseCharData(m_headerStamp, mig->version_number    , VERSION_NUMBER_LEN             , VERSION_NUMBER_OFFSET             , '0');
  sprintf(str,"%u", mig->header_size);
  parseCharData(m_headerStamp, str                    , HEADER_SIZE_LEN                , HEADER_SIZE_OFFSET                , '0');
  parseCharData(m_headerStamp, mig->checksum_algorithm, alb0100::CHECKSUM_ALGORITHM_LEN, alb0100::CHECKSUM_ALGORITHM_OFFSET, ' ');   
  sprintf(str,"%lu", mig->tape_mark_count);
  parseCharData(m_headerStamp, str                    , alb0100::TAPE_MARK_COUNT_LEN   , alb0100::TAPE_MARK_COUNT_OFFSET   , '0');
  m_block_size =  mig->block_size;
  sprintf(str,"%u", mig->block_size);
  parseCharData(m_headerStamp, str                    , alb0100::BLOCK_SIZE_LEN        , alb0100::BLOCK_SIZE_OFFSET        , '0');    
  parseCharData(m_headerStamp, mig->stager_version    , alb0100::STAGER_VERSION_LEN    , alb0100::STAGER_VERSION_OFFSET    , ' ');
  parseCharData(m_headerStamp, mig->stager_host       , alb0100::STAGER_HOST_LEN       , alb0100::STAGER_HOST_OFFSET       , ' ');
  parseCharData(m_headerStamp, mig->drive_name        , alb0100::DRIVE_NAME_LEN        , alb0100::DRIVE_NAME_OFFSET        , ' ');    
  parseCharData(m_headerStamp, mig->drive_serial      , alb0100::DRIVE_SERIAL_LEN      , alb0100::DRIVE_SERIAL_OFFSET      , ' ');
  parseCharData(m_headerStamp, mig->drive_firmware    , alb0100::DRIVE_FIRMWARE_LEN    , alb0100::DRIVE_FIRMWARE_OFFSET    , ' ');
  parseCharData(m_headerStamp, mig->drive_host        , alb0100::DRIVE_HOST_LEN        , alb0100::DRIVE_HOST_OFFSET        , ' ');
  parseCharData(m_headerStamp, mig->vol_density       , alb0100::VOL_DENSITY_LEN       , alb0100::VOL_DENSITY_OFFSET       , ' ');
  parseCharData(m_headerStamp, mig->vol_id            , alb0100::VOL_ID_LEN            , alb0100::VOL_ID_OFFSET            , ' ');
  parseCharData(m_headerStamp, mig->vol_serial        , alb0100::VOL_SERIAL_LEN        , alb0100::VOL_SERIAL_OFFSET        , ' ');
  parseCharData(m_headerStamp, mig->device_group_name , alb0100::DEVICE_GROUP_NAME_LEN , alb0100::DEVICE_GROUP_NAME_OFFSET , ' ');
   
  m_payload_size = mig->block_size - mig->header_size;   
 
}


//------------------------------------------------------------------------------------
//  startFile
//------------------------------------------------------------------------------------
void castor::tape::format::ALB0100Marshaller::startFile(Header &data)
throw(castor::exception::Exception) {
    
  char        str[31];
  FileHeader *file;

  // Dynamic cast to the current header structure version
  try{
   file = dynamic_cast<FileHeader*>(&data);
  }catch(std::bad_cast e){
    castor::exception::Exception ex(EINVAL);
    ex.getMessage()<<e.what() << "Failed to dynamic cast header to FileHeader";
    throw ex;
  }
  //Reset the progressive checksum
  m_file_progressive_checksum = adler32(0L,Z_NULL,0);
  m_file_block_count = 0;
    
  m_file_size = file->file_size;  // Part of the object status
  sprintf(str,"%lu", file->file_size);
  parseCharData(m_headerStamp, str               , alb0100::FILE_SIZE_LEN    , alb0100::FILE_SIZE_OFFSET    , '0');
  m_file_checksum = file->file_checksum;
  sprintf(str,"%u", file->file_checksum);
  parseCharData(m_headerStamp, str               , alb0100::FILE_CHECKSUM_LEN, alb0100::FILE_CHECKSUM_OFFSET, '0');
  parseCharData(m_headerStamp, file->file_ns_host, alb0100::FILE_NS_HOST_LEN , alb0100::FILE_NS_HOST_OFFSET , ' ');
  sprintf(str,"%lu", file->file_ns_id);
  parseCharData(m_headerStamp, str               , alb0100::FILE_NS_ID_LEN   , alb0100::FILE_NS_ID_OFFSET   , '0');
  parseCharData(m_headerStamp, file->file_name   , alb0100::FILE_NAME_LEN    , alb0100::FILE_NAME_OFFSET    , ' ');
    
}

//------------------------------------------------------------------------------------
//  marshall
//------------------------------------------------------------------------------------
void castor::tape::format::ALB0100Marshaller::marshall(char* block){

  char     str[21];
  uint64_t timestamp;
       
  sprintf(str,"%lu", m_block_count);
  parseCharData(m_headerStamp, str, alb0100::BLOCK_COUNT_LEN              , alb0100::BLOCK_COUNT_OFFSET              , '0');
  m_block_count++;
  timestamp = time(NULL);
  sprintf(str,"%lu", timestamp);
  parseCharData(m_headerStamp, str, alb0100::BLOCK_TIME_STAMP_LEN         , alb0100::BLOCK_TIME_STAMP_OFFSET         , '0');
  sprintf(str,"%lu", m_file_block_count);
  parseCharData(m_headerStamp, str, alb0100::FILE_BLOCK_COUNT_LEN         , alb0100::FILE_BLOCK_COUNT_OFFSET         , '0');
  m_file_block_count += m_payload_size;
  // Test for the last block of the file 
  if(m_file_block_count >=  m_file_size){// Check the file ckecksum is correct
    uint32_t temp_checksum  = adler32( m_file_progressive_checksum, (Bytef*)(block+HEADER_SIZE), m_payload_size-(m_file_block_count- m_file_size));
  { //----------------------
    char str[40];
    memcpy(str, m_headerStamp+alb0100::FILE_NAME_OFFSET ,40 );
    str[40]='\0';
    std::cout<<"Filename: "<<str <<" Checksum: "<< temp_checksum <<std::endl;
   }//----------------------
   if(m_file_checksum != temp_checksum){// RISE AN EXCERPTION
      castor::exception::Exception ex(EINVAL);
      ex.getMessage() << "File checksum mistmach.";
      throw ex;

    }
  }
  m_file_progressive_checksum  = adler32( m_file_progressive_checksum, (Bytef*)(block+HEADER_SIZE), m_payload_size);// payload progressive checksum
  sprintf(str,"%u", m_file_progressive_checksum);
  parseCharData(m_headerStamp, str, alb0100::FILE_PROGRESSIVE_CHECKSUM_LEN, alb0100::FILE_PROGRESSIVE_CHECKSUM_OFFSET, '0');
  memset(m_headerStamp+alb0100::HEADER_CHECKSUM_OFFSET, ' ', alb0100::HEADER_CHECKSUM_LEN ); // Set the memory to 10x' '
  m_header_checksum = adler32(adler32(0L,Z_NULL,0), (Bytef*)m_headerStamp, HEADER_SIZE); // Compute checksum on the header
  sprintf(str,"%u", m_header_checksum);
  parseCharData(m_headerStamp, str, alb0100::HEADER_CHECKSUM_LEN          , alb0100::HEADER_CHECKSUM_OFFSET          , '0');


  // Copy my header block in the 256KiB block provided
  memcpy(block, m_headerStamp, HEADER_SIZE); 
    
  //printHeader(m_headerStamp);     // visual return!!!
 
}

//------------------------------------------------------------------------------------
//  printHeader
//------------------------------------------------------------------------------------
void castor::tape::format::ALB0100Marshaller::printHeader(char *buff){
    
  using std::cout;
  using std::endl;
  
  char str[31];

  memset(str, ' ', 31);

  memcpy(str, buff+VERSION_NUMBER_OFFSET, VERSION_NUMBER_LEN );
  cout << str <<',' ;
  memset(str, ' ', 31);
    
  memcpy(str, buff+HEADER_SIZE_OFFSET, HEADER_SIZE_LEN);
  cout << str << ',' ;
  memset(str, ' ', 31);
    
  memcpy(str, buff+alb0100::CHECKSUM_ALGORITHM_OFFSET ,alb0100::CHECKSUM_ALGORITHM_LEN );
  cout << str << ',' ;
  memset(str, ' ', 31);
    
  memcpy(str, buff+alb0100::HEADER_CHECKSUM_OFFSET ,alb0100::HEADER_CHECKSUM_LEN );
  cout << str << ',' ;
  memset(str, ' ', 31);
    
  memcpy(str, buff+alb0100::TAPE_MARK_COUNT_OFFSET ,alb0100::TAPE_MARK_COUNT_LEN );
  cout << str << endl ;
  memset(str, ' ', 31);
    
  memcpy(str, buff+alb0100::BLOCK_SIZE_OFFSET ,alb0100::BLOCK_SIZE_LEN );
  cout << str << ',' ;
  memset(str, ' ', 31);
    
  memcpy(str, buff+alb0100::BLOCK_COUNT_OFFSET ,alb0100::BLOCK_COUNT_LEN );
  cout << str << ',' ;
  memset(str, ' ', 31);
    
  memcpy(str, buff+alb0100::BLOCK_TIME_STAMP_OFFSET ,alb0100::BLOCK_TIME_STAMP_LEN );
  cout << str << ',' ;
  memset(str, ' ', 31);
    
  memcpy(str, buff+alb0100::STAGER_VERSION_OFFSET ,alb0100::STAGER_VERSION_LEN );
  cout << str << ',' ;
  memset(str, ' ', 31);
    
  memcpy(str, buff+alb0100::STAGER_HOST_OFFSET, alb0100::STAGER_HOST_LEN);
  cout << str << endl;
  memset(str, ' ', 31);
    
  memcpy(str, buff+alb0100::DRIVE_NAME_OFFSET ,alb0100::DRIVE_NAME_LEN );
  cout << str << ',' ;
  memset(str, ' ', 31);
    
  memcpy(str, buff+alb0100::DRIVE_SERIAL_OFFSET ,alb0100::DRIVE_SERIAL_LEN );
  cout << str << ',' ;
  memset(str, ' ', 31);
    
  memcpy(str, buff+alb0100::DRIVE_FIRMWARE_OFFSET ,alb0100::DRIVE_FIRMWARE_LEN );
  cout << str << ',' ;
  memset(str, ' ', 31);
    
  memcpy(str, buff+alb0100::DRIVE_HOST_OFFSET ,alb0100::DRIVE_HOST_LEN );
  cout << str << ',' ;
  memset(str, ' ', 31);
    
  memcpy(str, buff+alb0100::VOL_DENSITY_OFFSET ,alb0100::VOL_DENSITY_LEN );
  cout << str << endl ;
  memset(str, ' ', 31);

  memcpy(str, buff+alb0100::VOL_ID_OFFSET ,alb0100::VOL_ID_LEN );
  cout << str << ',' ;
  memset(str, ' ', 31);
    
  memcpy(str, buff+alb0100::VOL_SERIAL_OFFSET ,alb0100::VOL_SERIAL_LEN );
  cout << str << ',' ;
  memset(str, ' ', 31);
    
  memcpy(str, buff+alb0100::DEVICE_GROUP_NAME_OFFSET ,alb0100::DEVICE_GROUP_NAME_LEN );
  cout << str << ',' ;
  memset(str, ' ', 31);
    
  memcpy(str, buff+alb0100::FILE_SIZE_OFFSET ,alb0100::FILE_SIZE_LEN );
  cout << str << ',' ;
  memset(str, ' ', 31);
    
  memcpy(str, buff+alb0100::FILE_CHECKSUM_OFFSET ,alb0100::FILE_CHECKSUM_LEN );
  cout << str << endl ;
  memset(str, ' ', 31);
    
  memcpy(str, buff+alb0100::FILE_NS_HOST_OFFSET ,alb0100::FILE_NS_HOST_LEN );
  cout << str << ',' ;
  memset(str, ' ', 31);
    
  memcpy(str, buff+alb0100::FILE_NS_ID_OFFSET ,alb0100::FILE_NS_ID_LEN );
  cout << str << ',' ;
  memset(str, ' ', 31);
    
  memcpy(str, buff+alb0100::FILE_PROGRESSIVE_CHECKSUM_OFFSET ,alb0100::FILE_PROGRESSIVE_CHECKSUM_LEN );
  cout << str << ',' ;
  memset(str, ' ', 31);
    
  memcpy(str, buff+alb0100::FILE_BLOCK_COUNT_OFFSET ,alb0100::FILE_BLOCK_COUNT_LEN );
  cout << str << ',' ;
  memset(str, ' ', 31);
    
  memcpy(str, buff+alb0100::FILE_NAME_OFFSET ,31 );
  cout << str << endl << endl;
}



