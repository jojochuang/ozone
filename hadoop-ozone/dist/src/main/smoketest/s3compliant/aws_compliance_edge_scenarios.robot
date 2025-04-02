*** Settings ***
Documentation     Tests for AWS compliance edge cases not yet covered.
Library           S3Library
Library           Collections

*** Variables ***
${MIN_BUCKET}           ab
${MAX_BUCKET}           ${EMPTY}    # Will generate a 63-character bucket name in test
${UPPER_BUCKET}         TestBucketUpper
${TRAILING_BUCKET}      bucket-
${LOCATION}             us-west-2
${PAGINATION_BUCKET}    pagination-bucket
${SPECIAL_BUCKET}       special-key-bucket
${COND_BUCKET}          conditional-bucket
${MULTIPART_BUCKET}     multipart-bucket

*** Test Cases ***
Test Minimum Length Bucket Name
    [Documentation]    Create a bucket with minimum length (2 characters) and verify creation.
    ${result}=    Create Bucket    ${MIN_BUCKET}
    Should Be True    ${result.success}
    Delete Bucket    ${MIN_BUCKET}

Test Maximum Length Bucket Name
    [Documentation]    Create a bucket with maximum length (63 characters) and verify creation.
    ${max_bucket}=    Generate Random Lowercase String    63
    ${result}=    Create Bucket    ${max_bucket}
    Should Be True    ${result.success}
    Delete Bucket    ${max_bucket}

Test Bucket Name With Uppercase
    [Documentation]    Attempt to create a bucket with uppercase letters and verify error.
    ${result}=    Create Bucket    ${UPPER_BUCKET}    expect_error=True
    Should Contain    ${result.error}    InvalidBucketName

Test Bucket Name With Trailing Dash
    [Documentation]    Attempt to create a bucket with a trailing dash and verify error.
    ${result}=    Create Bucket    ${TRAILING_BUCKET}    expect_error=True
    Should Contain    ${result.error}    InvalidBucketName

Test Bucket Creation With Location Constraint
    [Documentation]    Attempt to create a bucket with a LocationConstraint. Expect error or default handling.
    ${result}=    Create Bucket With Location    location-constraint-bucket    ${LOCATION}    expect_error=True
    Should Contain    ${result.error}    LocationConstraint

Test Bucket Already Owned By You
    [Documentation]    Simulate multi-user scenario: first user creates bucket, second user’s creation attempt fails.
    ${bucket}=    Set Variable    owned-bucket
    Create Bucket    ${bucket}    credentials=user1
    ${result}=    Create Bucket    ${bucket}    credentials=user2    expect_error=True
    Should Contain    ${result.error}    BucketAlreadyOwnedByYou
    Delete Bucket    ${bucket}    credentials=user1

Test List Objects Pagination
    [Documentation]    Upload >1000 objects and verify that listing returns a truncated result with a continuation token.
    Create Bucket    ${PAGINATION_BUCKET}
    : FOR    ${i}    IN RANGE    1100
    \    ${object_key}=    Set Variable    object-${i}
    \    Put Object    ${PAGINATION_BUCKET}    ${object_key}    data=TestData
    ${response}=    List Objects V2    ${PAGINATION_BUCKET}    MaxKeys=1000
    Should Be True    ${response.isTruncated}
    Should Not Be Empty    ${response.NextContinuationToken}
    Delete Bucket Recursively    ${PAGINATION_BUCKET}

Test Special Character Object Keys
    [Documentation]    PUT and GET objects whose keys include spaces, special symbols, and unicode.
    Create Bucket    ${SPECIAL_BUCKET}
    ${keys}=    Create List    my key    foo?bar#baz    测试键
    : FOR    ${key}    IN    @{keys}
    \    Put Object    ${SPECIAL_BUCKET}    ${key}    data=SpecialData
    : FOR    ${key}    IN    @{keys}
    \    ${data}=    Get Object    ${SPECIAL_BUCKET}    ${key}
    \    Should Be Equal    ${data}    SpecialData
    Delete Bucket    ${SPECIAL_BUCKET}

Test Conditional Requests
    [Documentation]    Verify conditional GET/HEAD using If-Match, If-None-Match, and If-Modified-Since.
    Create Bucket    ${COND_BUCKET}
    ${object_key}=    Set Variable    test-object
    Put Object    ${COND_BUCKET}    ${object_key}    data=ConditionalTest
    ${etag}=    Get Object ETag    ${COND_BUCKET}    ${object_key}
    # Conditional GET with matching ETag should succeed
    ${data}=    Get Object    ${COND_BUCKET}    ${object_key}    headers=If-Match:${etag}
    Should Be Equal    ${data}    ConditionalTest
    # Conditional GET with If-None-Match should return 304 Not Modified
    Get Object    ${COND_BUCKET}    ${object_key}    headers=If-None-Match:${etag}    expect_status=304
    ${last_modified}=    Get Object Last Modified    ${COND_BUCKET}    ${object_key}
    Head Object    ${COND_BUCKET}    ${object_key}    headers=If-Modified-Since:${last_modified}    expect_status=304
    Delete Bucket    ${COND_BUCKET}

Test Overlapping Multipart Uploads
    [Documentation]    Simulate overlapping uploads by uploading the same part number twice and ensuring the latest is used.
    Create Bucket    ${MULTIPART_BUCKET}
    ${object_key}=    Set Variable    multipart-object
    ${upload_id}=    Initiate Multipart Upload    ${MULTIPART_BUCKET}    ${object_key}
    # Upload part 1 twice (simulate overlap); second upload should overwrite
    Upload Part    ${MULTIPART_BUCKET}    ${object_key}    ${upload_id}    partNumber=1    data=PartOne
    Upload Part    ${MULTIPART_BUCKET}    ${object_key}    ${upload_id}    partNumber=1    data=PartOneUpdated
    Complete Multipart Upload    ${MULTIPART_BUCKET}    ${object_key}    ${upload_id}    parts=1
    ${result}=    Get Object    ${MULTIPART_BUCKET}    ${object_key}
    Should Be Equal    ${result}    PartOneUpdated
    Delete Bucket    ${MULTIPART_BUCKET}

