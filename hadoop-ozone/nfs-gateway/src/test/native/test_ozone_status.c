/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "fsal.h"
#include "FSAL/fsal_commonlib.h"

#include <stdio.h>

fsal_status_t ozone_status(uint32_t status);

int main(void)
{
	static const struct {
		uint32_t nfs_status;
		fsal_errors_t fsal_status;
	} cases[] = {
		{0, ERR_FSAL_NO_ERROR},
		{2, ERR_FSAL_NOENT},
		{5, ERR_FSAL_IO},
		{13, ERR_FSAL_ACCESS},
		{17, ERR_FSAL_EXIST},
		{20, ERR_FSAL_NOTDIR},
		{21, ERR_FSAL_ISDIR},
		{22, ERR_FSAL_INVAL},
		{66, ERR_FSAL_NOTEMPTY},
		{69, ERR_FSAL_DQUOT},
		{70, ERR_FSAL_STALE},
		{10001, ERR_FSAL_BADHANDLE},
		{10003, ERR_FSAL_BADCOOKIE},
		{10004, ERR_FSAL_NOTSUPP},
		{10008, ERR_FSAL_DELAY},
		{10041, ERR_FSAL_BADNAME},
		{10046, ERR_FSAL_FILE_OPEN},
	};
	size_t i;

	for (i = 0; i < sizeof(cases) / sizeof(cases[0]); ++i) {
		fsal_status_t status = ozone_status(cases[i].nfs_status);
		if (status.major != cases[i].fsal_status || status.minor != 0) {
			fprintf(stderr, "NFS status %u mapped to FSAL status %d:%d, expected %d:0\n",
				cases[i].nfs_status, status.major, status.minor, cases[i].fsal_status);
			return 1;
		}
	}

	if (ozone_status(99999).major != ERR_FSAL_SERVERFAULT) {
		fprintf(stderr, "Unknown NFS status did not map to ERR_FSAL_SERVERFAULT\n");
		return 1;
	}
	return 0;
}
@@
 		{10003, ERR_FSAL_BADCOOKIE},
+		{10027, ERR_FSAL_NOT_SAME},
 		{10004, ERR_FSAL_NOTSUPP},
