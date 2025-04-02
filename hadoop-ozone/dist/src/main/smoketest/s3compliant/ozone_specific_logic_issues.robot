*** Settings ***
Documentation     Tests for Ozone-specific behavior and implementation issues.
Library           S3Library

*** Variables ***
${RESERVED_BUCKET}    conf
${ACL_BUCKET}         acl-operation-bucket
${MALFORMED_BUCKET}   malformed-auth-bucket

*** Test Cases ***
Test Reserved Namespace Behavior
    [Documentation]    Create a bucket with a reserved name and verify that S3 operations work normally.
    Create Bucket    ${RESERVED_BUCKET}
    Put Object    ${RESERVED_BUCKET}    test-object    data=ReservedNS
    ${data}=    Get Object    ${RESERVED_BUCKET}    test-object
    Should Be Equal    ${data}    ReservedNS
    Delete Bucket    ${RESERVED_BUCKET}

Test Put Bucket ACL Returns Error
    [Documentation]    Attempt to set a bucket ACL and expect an error or a “not implemented” response.
    Create Bucket    ${ACL_BUCKET}
    ${result}=    Put Bucket ACL    ${ACL_BUCKET}    acl=public-read    expect_error=True
    Should Contain    ${result.error}    NotImplemented
    Delete Bucket    ${ACL_BUCKET}

Test Put Object ACL Returns Error
    [Documentation]    Attempt to set an object ACL and expect an error.
    Create Bucket    ${ACL_BUCKET}
    Put Object    ${ACL_BUCKET}    test-object    data=DataForACL
    ${result}=    Put Object ACL    ${ACL_BUCKET}    test-object    acl=public-read    expect_error=True
    Should Contain    ${result.error}    NotImplemented
    Delete Bucket    ${ACL_BUCKET}

Test Malformed Signature Handling
    [Documentation]    Send a request with a malformed signature and verify that a 403 Forbidden response is returned.
    ${result}=    Get Object    ${MALFORMED_BUCKET}    some-object    headers=Authorization:MalformedSignature    expect_error=True
    Should Contain    ${result.error}    403

