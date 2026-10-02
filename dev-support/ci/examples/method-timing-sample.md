# Sample method timing output (synthetic Surefire XML)

Produced by `analyze_surefire_method_timing.py` unit test fixture; use the same command against local `surefire-reports/` after running a single test class.

| Seconds | Class | Method | Module |
|---------|-------|--------|--------|
| 12.500 | `org.apache.hadoop.ozone.om.TestFoo` | `slow` | surefire-reports |
| 0.100 | `org.apache.hadoop.ozone.om.TestFoo` | `fast` | surefire-reports |
