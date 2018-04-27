# S3 Rollback

```
Assume role prior to execution with "export AWS_PROFILE=<profile>"
node rollback.js --bucket <bucket> --file <file> --datetime YYYY-MM-DDTHH:MM:SS [--dryrun, --verify]
--bucket        Bucket to perform the rollback operation on
--file          Location of file containing references to objects should be formatted as an S3 URI,
                i.e. s3://bucket/path/to/object. Separate objects should be separated by a new line.
--datetime      Threshold after which files are reverted, not including those on the precise second; format is of form YYYY-MM-DDTHH:MM:SS
--dryrun        Do not perform any write operations, simply log operations which would occur
--verify        List the object versions which fit the parameters after rollback to double check all operations were successful
```
