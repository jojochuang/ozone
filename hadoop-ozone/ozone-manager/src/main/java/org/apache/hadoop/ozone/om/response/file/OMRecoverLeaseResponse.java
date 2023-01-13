package org.apache.hadoop.ozone.om.response.file;

import org.apache.hadoop.hdds.utils.db.BatchOperation;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.response.CleanupTableInfo;
import org.apache.hadoop.ozone.om.response.key.OmKeyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
        .OMResponse;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;

import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.FILE_TABLE;
import static org.apache.hadoop.ozone.om.OmMetadataManagerImpl.OPEN_FILE_TABLE;

@CleanupTableInfo(cleanupTables = {FILE_TABLE, OPEN_FILE_TABLE})
public class OMRecoverLeaseResponse extends OmKeyResponse {

    private String openKeyName;
    public OMRecoverLeaseResponse(@Nonnull OMResponse omResponse,
        BucketLayout bucketLayout, String openKeyName) {
        super(omResponse, bucketLayout);
        checkStatusNotOK();
        this.openKeyName = openKeyName;
    }

    @Override
    protected void addToDBBatch(OMMetadataManager omMetadataManager,
                                BatchOperation batchOperation) throws IOException {
        // Delete from OpenKey table
        omMetadataManager.getOpenKeyTable(getBucketLayout())
            .deleteWithBatch(batchOperation, openKeyName);
    }

    @Override
    public BucketLayout getBucketLayout() {
        return BucketLayout.FILE_SYSTEM_OPTIMIZED;
    }
}
