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

fsal_status_t ozone_status(uint32_t status)
{
	switch (status) {
	case 0:
		return fsalstat(ERR_FSAL_NO_ERROR, 0);
	case 13: /* NFS4ERR_ACCESS */
		return fsalstat(ERR_FSAL_ACCESS, 0);
	case 5: /* NFS4ERR_IO */
		return fsalstat(ERR_FSAL_IO, 0);
	case 17: /* NFS4ERR_EXIST */
		return fsalstat(ERR_FSAL_EXIST, 0);
	case 2: /* NFS4ERR_NOENT */
		return fsalstat(ERR_FSAL_NOENT, 0);
	case 70: /* NFS4ERR_STALE */
		return fsalstat(ERR_FSAL_STALE, 0);
	case 10001: /* NFS4ERR_BADHANDLE */
		return fsalstat(ERR_FSAL_BADHANDLE, 0);
	case 10003: /* NFS4ERR_BAD_COOKIE */
		return fsalstat(ERR_FSAL_BADCOOKIE, 0);
	case 10004: /* NFS4ERR_NOTSUPP */
		return fsalstat(ERR_FSAL_NOTSUPP, 0);
	case 20: /* NFS4ERR_NOTDIR */
		return fsalstat(ERR_FSAL_NOTDIR, 0);
	case 21: /* NFS4ERR_ISDIR */
		return fsalstat(ERR_FSAL_ISDIR, 0);
	case 22: /* NFS4ERR_INVAL */
		return fsalstat(ERR_FSAL_INVAL, 0);
	case 66: /* NFS4ERR_NOTEMPTY */
		return fsalstat(ERR_FSAL_NOTEMPTY, 0);
	case 69: /* NFS4ERR_DQUOT */
		return fsalstat(ERR_FSAL_DQUOT, 0);
	case 10008: /* NFS4ERR_DELAY */
		return fsalstat(ERR_FSAL_DELAY, 0);
	case 10041: /* NFS4ERR_BADNAME */
		return fsalstat(ERR_FSAL_BADNAME, 0);
	case 10046: /* NFS4ERR_FILE_OPEN */
		return fsalstat(ERR_FSAL_FILE_OPEN, 0);
	default:
		return fsalstat(ERR_FSAL_SERVERFAULT, 0);
	}
}
@@
 	case 10003: /* NFS4ERR_BAD_COOKIE */
 		return fsalstat(ERR_FSAL_BADCOOKIE, 0);
+	case 10027: /* NFS4ERR_NOT_SAME */
+		return fsalstat(ERR_FSAL_NOT_SAME, 0);
 	case 10004: /* NFS4ERR_NOTSUPP */
