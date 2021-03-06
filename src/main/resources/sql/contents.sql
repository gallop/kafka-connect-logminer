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

SELECT
    THREAD#,
    SCN,
    START_SCN,
    COMMIT_SCN,
    to_timestamp(TIMESTAMP) TIMESTAMP,
    OPERATION_CODE,
    OPERATION,
    SEG_OWNER,
    TABLE_NAME,
    USERNAME,
    SQL_REDO,
    ROW_ID,
    CSF,
    TABLE_SPACE,
    SEQUENCE#,
    TX_NAME,
    SEG_NAME
FROM
    V$LOGMNR_CONTENTS WHERE OPERATION_CODE IN (
        1,
        2,
        3
    ) and
