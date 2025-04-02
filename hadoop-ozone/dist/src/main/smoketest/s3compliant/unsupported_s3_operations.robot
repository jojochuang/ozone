*** Settings ***
Documentation     Tests for S3 operations that are currently unsupported in Ozone.
Library           S3Library

*** Variables ***
${VERSION_BUCKET}          versioning-bucket
${POLICY_BUCKET}           policy-bucket
${CORS_BUCKET}             cors-bucket
${LOGGING_BUCKET}          logging-bucket
${WEBSITE_BUCKET}          website-bucket
${SSE_BUCKET}              sse-bucket
${SELECT_BUCKET}           select-bucket
${MULTIPART_LIST_BUCKET}   multipart-list-bucket
${LOCK_BUCKET}             lock-bucket
${OBJECT_KEY}              test-object

*** Test Cases ***
Test Bucket Versioning Enablement
    [Documentation]    Enable versioning on a bucket and verify that versioning status is active.
    Create Bucket    ${VERSION_BUCKET}
    Put Bucket Versioning    ${VERSION_BUCKET}    Status=Enabled
    ${status}=    Get Bucket Versioning    ${VERSION_BUCKET}
    Should Be Equal    ${status}    Enabled
    Delete Bucket    ${VERSION_BUCKET}

Test Listing Object Versions
    [Documentation]    Upload an object, overwrite it, and then list object versions.
    Create Bucket    ${VERSION_BUCKET}
    Put Object    ${VERSION_BUCKET}    ${OBJECT_KEY}    data=Version1
    Put Object    ${VERSION_BUCKET}    ${OBJECT_KEY}    data=Version2
    ${versions}=    List Object Versions    ${VERSION_BUCKET}    ${OBJECT_KEY}
    Length Should Be    ${versions}    2
    Delete Bucket    ${VERSION_BUCKET}

Test Bucket Policy Set And Get
    [Documentation]    Set a bucket policy and verify that it can be retrieved.
    Create Bucket    ${POLICY_BUCKET}
    ${policy}=    Create Dictionary    Version    "2012-10-17"    Statement    [{"Effect": "Allow", "Principal": "*", "Action": "s3:GetObject", "Resource": "arn:aws:s3:::${POLICY_BUCKET}/*"}]
    Put Bucket Policy    ${POLICY_BUCKET}    ${policy}
    ${returned_policy}=    Get Bucket Policy    ${POLICY_BUCKET}
    Should Be Equal    ${returned_policy}    ${policy}
    Delete Bucket    ${POLICY_BUCKET}

Test Bucket CORS Configuration
    [Documentation]    Set and then retrieve a bucket CORS configuration.
    Create Bucket    ${CORS_BUCKET}
    ${cors}=    Create Dictionary    CORSRules    [{"AllowedOrigin": "*", "AllowedMethod": "GET", "MaxAgeSeconds": 3000}]
    Put Bucket CORS    ${CORS_BUCKET}    ${cors}
    ${returned_cors}=    Get Bucket CORS    ${CORS_BUCKET}
    Should Be Equal    ${returned_cors}    ${cors}
    Delete Bucket    ${CORS_BUCKET}

Test Bucket Logging Configuration
    [Documentation]    Set bucket logging configuration and verify retrieval.
    Create Bucket    ${LOGGING_BUCKET}
    ${logging}=    Create Dictionary    LoggingEnabled    {"TargetBucket": "log-target", "TargetPrefix": "logs/"}
    Put Bucket Logging    ${LOGGING_BUCKET}    ${logging}
    ${returned_logging}=    Get Bucket Logging    ${LOGGING_BUCKET}
    Should Be Equal    ${returned_logging}    ${logging}
    Delete Bucket    ${LOGGING_BUCKET}

Test Static Website Hosting Configuration
    [Documentation]    Set website hosting configuration on a bucket and verify.
    Create Bucket    ${WEBSITE_BUCKET}
    ${website}=    Create Dictionary    IndexDocument    index.html    ErrorDocument    error.html
    Put Bucket Website    ${WEBSITE_BUCKET}    ${website}
    ${returned_website}=    Get Bucket Website    ${WEBSITE_BUCKET}
    Should Be Equal    ${returned_website}    ${website}
    Delete Bucket    ${WEBSITE_BUCKET}

Test Server-Side Encryption
    [Documentation]    Upload an object with server-side encryption and verify that the encryption header is present.
    Create Bucket    ${SSE_BUCKET}
    Put Object    ${SSE_BUCKET}    ${OBJECT_KEY}    data=EncryptedData    headers=x-amz-server-side-encryption:AES256
    ${metadata}=    Head Object    ${SSE_BUCKET}    ${OBJECT_KEY}
    Should Contain    ${metadata.headers}    x-amz-server-side-encryption
    Delete Bucket    ${SSE_BUCKET}

Test S3 Select Query
    [Documentation]    Use S3 Select to query content from an object.
    Create Bucket    ${SELECT_BUCKET}
    ${csv_data}=    Catenate    SEPARATOR=\n    "name,age"    "Alice,30"    "Bob,25"
    Put Object    ${SELECT_BUCKET}    ${OBJECT_KEY}    data=${csv_data}    ContentType=text/csv
    ${result}=    Select Object Content    ${SELECT_BUCKET}    ${OBJECT_KEY}    Expression=SELECT * FROM S3Object
    Should Contain    ${result}    Alice
    Should Contain    ${result}    Bob
    Delete Bucket    ${SELECT_BUCKET}

Test Multipart Upload Listing
    [Documentation]    Initiate a multipart upload and verify that it appears in the multipart uploads listing.
    Create Bucket    ${MULTIPART_LIST_BUCKET}
    ${upload_id}=    Initiate Multipart Upload    ${MULTIPART_LIST_BUCKET}    ${OBJECT_KEY}
    ${uploads}=    List Multipart Uploads    ${MULTIPART_LIST_BUCKET}
    List Should Contain Value    ${uploads}    ${upload_id}
    Abort Multipart Upload    ${MULTIPART_LIST_BUCKET}    ${OBJECT_KEY}    ${upload_id}
    Delete Bucket    ${MULTIPART_LIST_BUCKET}

Test Object Locking And Legal Hold
    [Documentation]    Upload an object with object lock and legal hold headers and verify they are set.
    Create Bucket    ${LOCK_BUCKET}
    ${headers}=    Create Dictionary    x-amz-object-lock-mode    GOVERNANCE    x-amz-object-lock-retain-until-date    2099-12-31T00:00:00Z    x-amz-object-lock-legal-hold    ON
    Put Object    ${LOCK_BUCKET}    ${OBJECT_KEY}    data=LockData    headers=${headers}
    ${lock_info}=    Get Object Lock Configuration    ${LOCK_BUCKET}    ${OBJECT_KEY}
    Dictionary Should Contain Value    ${lock_info}    GOVERNANCE
    Dictionary Should Contain Value    ${lock_info}    ON
    Delete Bucket    ${LOCK_BUCKET}

