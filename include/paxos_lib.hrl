
-define( UNDECIDED, '$_undecided_$'). 

-record( paxos_fsm_state, {subject, 
			   n=0, 
			   value = ?UNDECIDED,
			   all, 
			   quorum, 
			   current=0, 
			   others, 
			   init_n,
			   return_pids=[], 
			   prepared_n=0,
			   coordinator_module,
			   db_adapter
			  } ).




