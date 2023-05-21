# binary_record_db
Database that stores recursive records containing binary data fields

The purpose is to create a simple thread-safe database with the following properties

1. Only one record type is supported, Record, which contains Id (long), external Id (string), list of attributes and list of attachments (Records)
2. External Id (string) must be provided for each unique record
3. Records can be updated as a whole, partially, or deleted
4. All writes are appended to files to ensure data integrity in case of write failure
5. Changes are written to write-ahead file to ensure performance
6. Reads will include changes in write-ahead file
