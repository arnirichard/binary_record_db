# binary_record_db
Database that stores recursive records containing binary data fields

The purpose is to create a simple thread-safe database with the following properties

1. Only one record type is supported, Record, which contains Id (long), type (int) external Id (string), list of attributes and list of attachments or references (Records)
2. Records can be updated as a whole, partially, or deleted
3. All writes are appended to files to ensure data integrity in case of write failure
4. Changes are written to write-ahead file to ensure performance
5. Reads will include changes in write-ahead file
