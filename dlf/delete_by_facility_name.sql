 

delete from dlf_num_param_values 
  where exists
    (
    select * from dlf_messages
       where dlf_messages.facility=
               (select FAC_NO from dlf_facilities where FAC_NAME='&&facil')
               and dlf_messages.msg_seq_no=dlf_num_param_values.msg_seq_no
    );

delete  from dlf_str_param_values
  where exists
    (
    select * from dlf_messages
       where dlf_messages.facility=
                (select FAC_NO from dlf_facilities where FAC_NAME='&&facil')
                and dlf_messages.msg_seq_no=dlf_str_param_values.msg_seq_no
    );

delete  from dlf_rq_ids_map
  where exists
    (
    select * from dlf_messages
       where dlf_messages.facility=
                 (select FAC_NO from dlf_facilities where FAC_NAME='&&facil')
                 and dlf_messages.msg_seq_no=dlf_rq_ids_map.msg_seq_no
    );

delete  from dlf_tape_ids
  where exists
    (
    select * from dlf_messages
       where dlf_messages.facility=
                 (select FAC_NO from dlf_facilities where FAC_NAME='&&facil')
                 and dlf_messages.msg_seq_no=dlf_tape_ids.msg_seq_no
    );

delete  from dlf_messages where facility=
                 (select FAC_NO from dlf_facilities where FAC_NAME='&&facil');

undef facil;

