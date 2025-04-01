*** Settings

*** Variables

*** Keywords

*** Test Cases ***
S3 Operations On FSO Bucket Should Fail Or Be Unsupported
[Documentation]    Try S3 ops on FSO bucket (Ozone only supports OBS)
    Create FSO Bucket    ${BUCKET}-fso
    ${resp}=    Upload File    ${BUCKET}-fso    test.txt    ${EMPTY}
    Should Contain Any    ${resp.text}    NotImplemented    Operation not supported