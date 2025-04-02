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
S3 Select Should Return Not Implemented
    [Documentation]    Attempt SelectObjectContent call, which Ozone does not support
    ${xml}=    Set Variable    <SelectObjectContentRequest><Expression>SELECT * FROM S3Object</Expression></SelectObjectContentRequest>
    ${resp}=    POST    ${BASE_URL}/${BUCKET}/data.csv?select&select-type=2    data=${xml}
    Should Be True    ${resp.status_code} == 501 or 'NotImplemented' in ${resp.text}
