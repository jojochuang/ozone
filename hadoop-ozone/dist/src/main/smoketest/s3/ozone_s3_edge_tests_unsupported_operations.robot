*** Settings

*** Variables

*** Keywords

*** Test Cases ***
Put Bucket Versioning Should Fail
[Documentation]    Tries to enable versioning, expected to fail or return NotImplemented
    ${xml}=    Set Variable    <VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Enabled</Status></VersioningConfiguration>
    ${resp}=    Put Bucket Versioning    ${BUCKET}-versioned    ${xml}
    Should Contain Any    ${resp.text}    NotImplemented    Error    AccessDenied

Put Bucket Policy Should Fail
[Documentation]    Attempt to set a bucket policy (unsupported in Ozone)
    ${policy}=    Set Variable    {"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:*","Resource":"arn:aws:s3:::examplebucket/*"}]}
    ${resp}=    Put Bucket Policy    ${BUCKET}-policy    ${policy}
    Should Contain Any    ${resp.text}    NotImplemented    AccessDenied

Put Object ACL Should Fail Or Be Ignored
[Documentation]    Try to apply object ACL (likely unsupported or ignored)
    Create Bucket    ${BUCKET}-aclobj
    Upload File    ${BUCKET}-aclobj    object.txt    ${EMPTY}
    ${resp}=    Put Object ACL    ${BUCKET}-aclobj    object.txt
    Should Contain Any    ${resp.text}    NotImplemented    AccessDenied

Put Object With SSE Should Be Ignored Or Fail
[Documentation]    Attempt SSE header during PUT (Ozone likely ignores)
    Create Bucket    ${BUCKET}-sse
    Upload File With Headers    ${BUCKET}-sse    encrypted.txt    ${EMPTY}    x-amz-server-side-encryption=AES256
    ${resp}=    Head Object    ${BUCKET}-sse    encrypted.txt
    Should Not Contain    ${resp.headers}    x-amz-server-side-encryption