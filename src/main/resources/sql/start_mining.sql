/**
 * Copyright 2018 David Arnold
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

begin
    dbms_logmnr.add_logfile(logfilename=>'/opt/oracle/oradata/orcl/redo01.log',options=>dbms_logmnr.NEW);
    dbms_logmnr.add_logfile(logfilename=>'/opt/oracle/oradata/orcl/redo02.log',options=>dbms_logmnr.ADDFILE);
    dbms_logmnr.add_logfile(logfilename=>'/opt/oracle/oradata/orcl/redo03.log',options=>dbms_logmnr.ADDFILE);
	dbms_logmnr.start_logmnr(
	    dictfilename=>'/opt/oracle/oradata/orcl/LOGMNR/dictionary.ora',
	    OPTIONS =>
			DBMS_LOGMNR.COMMITTED_DATA_ONLY
			+DBMS_LOGMNR.NO_ROWID_IN_STMT
	);
end;