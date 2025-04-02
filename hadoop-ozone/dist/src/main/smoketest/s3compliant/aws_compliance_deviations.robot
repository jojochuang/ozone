*** Settings ***
Documentation     Tests for behaviors that deviate from strict AWS S3 compliance.
Library           S3Library

*** Variables ***
${ACL_BUCKET}           acl-bucket
${NONEXIST_BUCKET}      nonexist-bucket
${NONEXIST_OBJECT}      nonexist-object
${MULTIPART_ETAG_BUCKET}  multipart-etag-bucket
${COPY_OBJECT_BUCKET}     copy-bucket

*** Test Cases ***
Test Default Bucket ACL
    [Documentation]    Create a bucket and verify its ACL is private (owner-only), not granting extra group permissions.
    Create Bucket    ${ACL_BUCKET}
    ${acl}=    Get Bucket ACL    ${ACL_BUCKET}
    # Expect only owner-full-control; adjust assertion per expected AWS ACL structure.
    Dictionary Should Contain Value    ${acl}    FullControl
    Should Not Contain    ${acl}    Group
    Delete Bucket    ${ACL_BUCKET}

Test GET On Non-Existent Bucket/Object
    [Documentation]    Attempt GET on a non-existent bucket and object to verify that the error XML includes proper error codes.
    ${bucket_response}=    Get Bucket    ${NONEXIST_BUCKET}    expect_error=True
    Should Contain    ${bucket_response.error}    NoSuchBucket
    ${object_response}=    Get Object    ${NONEXIST_BUCKET}    ${NONEXIST_OBJECT}    expect_error=True
    Should Contain    ${object_response.error}    NoSuchKey

Test Multipart Upload ETag Computation
    [Documentation]    Initiate a multipart upload, complete it, and verify that the returned ETag matches the AWS MD5-with-part-count scheme.
    Create Bucket    ${MULTIPART_ETAG_BUCKET}
    ${object_key}=    Set Variable    multipart-etag-object
    ${upload_id}=    Initiate Multipart Upload    ${MULTIPART_ETAG_BUCKET}    ${object_key}
    Upload Part    ${MULTIPART_ETAG_BUCKET}    ${object_key}    ${upload_id}    partNumber=1    data=PartOneData
    Upload Part    ${MULTIPART_ETAG_BUCKET}    ${object_key}    ${upload_id}    partNumber=2    data=PartTwoData
    ${complete_response}=    Complete Multipart Upload    ${MULTIPART_ETAG_BUCKET}    ${object_key}    ${upload_id}    parts=2
    ${etag}=    Get From Dictionary    ${complete_response}    ETag
    ${expected_etag}=    Compute Multipart ETag    data_list=PartOneData,PartTwoData
    Should Be Equal    ${etag}    ${expected_etag}
    Delete Bucket    ${MULTIPART_ETAG_BUCKET}

Test Copy Object Self-Copy For Metadata Update
    [Documentation]    Copy an object onto itself to update metadata and verify that only the metadata is updated while the object content remains the same.
    Create Bucket    ${COPY_OBJECT_BUCKET}
    ${object_key}=    Set Variable    self-copy-object
    Put Object    ${COPY_OBJECT_BUCKET}    ${object_key}    data=OriginalData
    ${new_metadata}=    Create Dictionary    x-amz-meta-test    updated
    Copy Object    source_bucket=${COPY_OBJECT_BUCKET}    source_key=${object_key}    dest_bucket=${COPY_OBJECT_BUCKET}    dest_key=${object_key}    metadata=${new_metadata}    metadataDirective=REPLACE
    ${data}=    Get Object    ${COPY_OBJECT_BUCKET}    ${object_key}
    Should Be Equal    ${data}    OriginalData
    ${metadata}=    Get Object Metadata    ${COPY_OBJECT_BUCKET}    ${object_key}
    Dictionary Should Contain Value    ${metadata}    updated
    Delete Bucket    ${COPY_OBJECT_BUCKET}

