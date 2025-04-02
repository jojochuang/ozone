*** Settings ***
Documentation     Tests for edge cases unique to Ozone’s implementation.
Library           S3Library
Library           Collections

*** Variables ***
${FSO_BUCKET}           fso-bucket
${MULTIUSER_BUCKET}     multiuser-bucket
${CRED_ROT_BUCKET}      cred-rotation-bucket
${OVERSIZED_META}       ${EMPTY}    # Placeholder; generate metadata exceeding allowed size
${TOO_MANY_TAGS}        ${EMPTY}    # Placeholder; generate a list of >10 tags
${RESERVED_BUCKETS}     Create List    conf    logs    jmx

*** Test Cases ***
Test FSO Bucket S3 Access
    [Documentation]    Attempt S3 operations on a bucket created with FSO layout.
    # Create bucket with FSO flag (assume keyword supports a layout parameter)
    Create Bucket    ${FSO_BUCKET}    layout=FSO
    ${result}=    Put Object    ${FSO_BUCKET}    test-object    data=TestData    expect_error=True
    Should Contain    ${result.error}    UnsupportedBucketType
    Delete Bucket    ${FSO_BUCKET}

Test Multi-Tenant Access Control
    [Documentation]    Verify that a bucket created by one user is not accessible by another.
    Create Bucket    ${MULTIUSER_BUCKET}    credentials=userA
    Put Object    ${MULTIUSER_BUCKET}    secret-object    data=SecretData    credentials=userA
    ${result}=    Get Object    ${MULTIUSER_BUCKET}    secret-object    credentials=userB    expect_error=True
    Should Contain    ${result.error}    AccessDenied
    Delete Bucket    ${MULTIUSER_BUCKET}    credentials=userA

Test S3 Credential Rotation
    [Documentation]    Verify that after revoking credentials, S3 operations fail.
    ${creds}=    Generate S3 Credentials    bucket=${CRED_ROT_BUCKET}
    Create Bucket    ${CRED_ROT_BUCKET}    credentials=${creds}
    Put Object    ${CRED_ROT_BUCKET}    test-object    data=DataBeforeRevoke    credentials=${creds}
    Revoke S3 Credentials    ${creds}
    ${result}=    Get Object    ${CRED_ROT_BUCKET}    test-object    credentials=${creds}    expect_error=True
    Should Contain    ${result.error}    Forbidden
    Delete Bucket    ${CRED_ROT_BUCKET}

Test Multipart Upload With Oversized Metadata
    [Documentation]    Attempt to initiate a multipart upload with metadata exceeding allowed limits.
    Create Bucket    oversized-meta-bucket
    ${oversized}=    Generate Oversized Metadata    size=3000    # Assume 3000 bytes exceeds limit
    ${result}=    Initiate Multipart Upload With Metadata    oversized-meta-bucket    test-object    metadata=${oversized}    expect_error=True
    Should Contain    ${result.error}    MetadataTooLarge
    Delete Bucket    oversized-meta-bucket

Test Multipart Upload With Too Many Tags
    [Documentation]    Attempt to initiate a multipart upload with more than the allowed number of tags.
    Create Bucket    too-many-tags-bucket
    ${tags}=    Generate Tag List    count=15    # Assume limit is 10
    ${result}=    Initiate Multipart Upload With Tags    too-many-tags-bucket    test-object    tags=${tags}    expect_error=True
    Should Contain    ${result.error}    InvalidTag
    Delete Bucket    too-many-tags-bucket

Test Reserved Namespace Bucket Access
    [Documentation]    Create buckets with reserved names and verify that S3 operations are not conflicted by internal endpoints.
    : FOR    ${bucket}    IN    @{RESERVED_BUCKETS}
    \    Create Bucket    ${bucket}
    \    Put Object    ${bucket}    test-object    data=ReservedTest
    \    ${data}=    Get Object    ${bucket}    test-object
    \    Should Be Equal    ${data}    ReservedTest
    \    Delete Bucket    ${bucket}

