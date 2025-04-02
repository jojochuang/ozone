*** Settings ***
Library           OperatingSystem
Library           Collections
Library           RequestsLibrary
Library           String

*** Variables ***
${BASE_URL}       http://s3g:9878
${ACCESS_KEY}     your-access-key
${SECRET_KEY}     your-secret-key

*** Test Cases ***
S3 Control API Should Return Not Implemented
    [Documentation]    Try calling a fake S3 control endpoint (access points or batch operations)
    ${resp}=    GET    ${BASE_URL}/v20180820/accesspoint/my-ap
    Should Be True    ${resp.status_code} == 501 or 'NotImplemented' in ${resp.text}
