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
S3 Outposts Endpoint Should Fail
    [Documentation]    Simulate an Outposts-style bucket URL
    ${resp}=    GET    ${BASE_URL}/bucketname-accountid.outpostid/test.txt
    Should Be True    ${resp.status_code} == 501 or 'NotImplemented' in ${resp.text}
