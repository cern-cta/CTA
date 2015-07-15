SELECT /*+ INDEX_FFS(F I_fmd_stagertime_null) */ count(*)
  FROM cns_file_metadata F WHERE decode(nvl(stagertime, 1), 1, 1, NULL) = 1;
