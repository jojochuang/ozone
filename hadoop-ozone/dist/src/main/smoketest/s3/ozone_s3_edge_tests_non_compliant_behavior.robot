*** Settings

*** Variables

*** Keywords

*** Test Cases ***
Default Bucket ACL Should Be Private
[Documentation]    Check if default ACL grants group or world access (AWS default is private)
    Create Bucket    ${BUCKET}-acltest
    ${resp}=    Get Bucket ACL    ${BUCKET}-acltest
    Should Contain    ${resp.content}    <Owner>
    Should Not Contain    ${resp.content}    Group