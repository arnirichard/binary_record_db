# binary_record_db
Database that stores recursive records containing binary data fields

The purpose is to create a simple thread-safe database with the following properties

1. Only one record type is supported, Record, which contains Id (long), type (int), external Id (string), list of attributes and list of attachments or references (Records)
2. Attributes contains Type (int), and binary data byte[]
3. Record has state Full (independent record), Attachment (as part of parent record), Partial (partial change to record), Reference (pointer to record), Deleted (record to be deleted with attachments)
5. 2. Records can be updated as a whole, partially, or deleted
6. All writes are appended to files to ensure data integrity in case of write failure
7. Changes are written to write-ahead file to ensure performance
8. Reads will include changes in write-ahead file
