*** Settings

*** Variables

*** Keywords

*** Test Cases ***
Create Bucket With Invalid Names Should Fail
[Documentation]    Tests various invalid bucket names against AWS naming rules
    ${invalid_names}=    Create List    Ab    BUCKET    bucket-    bucket..name    bucket_name
    FOR    ${name}    IN    @{invalid_names}
        ${response}=    Create Bucket    ${name}
        Should Contain    ${response.content}    InvalidBucketName
    END