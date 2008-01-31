{
  for (i=1;i<=NF;i++) {

    # If the corrent field is the -V or -v switch
    if (index($i,"-V") != 0 || index($i,"-v") != 0 ) {

      # The next field should be the volume ID
      i++;
      vid=$i;

      if ($5 == "tpread") {
        access_mode=0; # WRITE_DISABLE=0
      } else {
        access_mode=1; # WRITE_ENABLE=1
      }

      printf("./rtcpc_test %s T10K60 \"\" \"\" %d\n", vid, access_mode);
    }
  }
}
