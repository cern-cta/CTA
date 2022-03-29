error() {
  echo "$*" >&2
  exit 1
}

[ $# -eq 2 ] || error "Expected 2 parameters, got $#. Usage: CreateAllSchemasSQL.sh CMAKE_CURRENT_SOURCE_DIR catalogue_schema_directory"

CMAKE_CURRENT_SOURCE_DIR=$1
catalogue_schema_directory=$2

cat ${CMAKE_CURRENT_SOURCE_DIR}/sqlite_catalogue_schema_header.sql \
    ${CMAKE_CURRENT_SOURCE_DIR}/common_catalogue_schema.sql \
    insert_cta_catalogue_version.sql \
    ${CMAKE_CURRENT_SOURCE_DIR}/sqlite_catalogue_schema_trailer.sql |\
    sed 's/--.*$//' |\
    # SQLite does not allow the use of functions in indexes
    sed 's/(LOWER\([^\)]*\))/\1/' |\
    sed 's/UINT8TYPE/INTEGER/g' |\
    sed 's/UINT16TYPE/INTEGER/g' |\
    sed 's/UINT32TYPE/INTEGER/g' |\
    sed 's/UINT64TYPE/INTEGER/g' |\
    sed 's/CHECKSUM_BLOB_TYPE/BLOB\(200\)/g' |\
    tee sqlite_catalogue_schema.sql ${catalogue_schema_directory}/sqlite_catalogue_schema.sql

cat ${CMAKE_CURRENT_SOURCE_DIR}/oracle_catalogue_schema_header.sql \
    ${CMAKE_CURRENT_SOURCE_DIR}/common_catalogue_schema.sql \
    insert_cta_catalogue_version.sql \
    ${CMAKE_CURRENT_SOURCE_DIR}/oracle_catalogue_schema_trailer.sql |\
    sed 's/--.*$//' |\
    sed 's/UINT8TYPE/NUMERIC\(3, 0\)/g' |\
    sed 's/UINT16TYPE/NUMERIC\(5, 0\)/g' |\
    sed 's/UINT32TYPE/NUMERIC\(10, 0\)/g' |\
    sed 's/UINT64TYPE/NUMERIC\(20, 0\)/g' |\
    sed 's/VARCHAR/VARCHAR2/g' |\
    sed 's/CHECKSUM_BLOB_TYPE/RAW\(200\)/g' |\
    tee oracle_catalogue_schema.sql ${catalogue_schema_directory}/oracle_catalogue_schema.sql

cat ${CMAKE_CURRENT_SOURCE_DIR}/postgres_catalogue_schema_header.sql \
    ${CMAKE_CURRENT_SOURCE_DIR}/common_catalogue_schema.sql \
    insert_cta_catalogue_version.sql \
    ${CMAKE_CURRENT_SOURCE_DIR}/postgres_catalogue_schema_trailer.sql |\
    sed 's/--.*$//' |\
    sed 's/UINT8TYPE/NUMERIC\(3, 0\)/g' |\
    sed 's/UINT16TYPE/NUMERIC\(5, 0\)/g' |\
    sed 's/UINT32TYPE/NUMERIC\(10, 0\)/g' |\
    sed 's/UINT64TYPE/NUMERIC\(20, 0\)/g' |\
    sed 's/CHECKSUM_BLOB_TYPE/BYTEA/g' |\
    tee postgres_catalogue_schema.sql ${catalogue_schema_directory}/postgres_catalogue_schema.sql
