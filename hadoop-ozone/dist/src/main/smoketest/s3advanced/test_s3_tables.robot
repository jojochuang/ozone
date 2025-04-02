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
S3 Tables Operation Should Be Unsupported
    [Documentation]    Simulate request to S3 Tables feature (e.g., Iceberg over S3)
    ${resp}=    GET    ${BASE_URL}/${BUCKET}/iceberg/table/metadata.json
    Should Be True    ${resp.status_code} == 501 or 'NotImplemented' in ${resp.text}
