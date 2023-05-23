using BinaryDB;
using NUnit.Framework.Internal;
using System.Diagnostics;
using System.Xml.Linq;

namespace BinaryDBTest
{
    public class BasicTests
    {
        const string DbName = "test";
        const string Folder = @"C:\Temp\DBTEST";

        [SetUp]
        public void Setup()
        {
        }

        [Test]
        public async Task BasicTestAttributes()
        {
            // 1. Create db from scratch
            BinaryDB.BinaryDB.DeleteDB(DbName, Folder);
            var db = await BinaryDB.BinaryDB.LoadOrCreateDBAsync(DbName, Folder);

            // 2. Write a record with a single attribute
            int type = 1;
            int fieldType = 2;
            byte[] attribute1Data = new byte[] { 0x01, 0x02 };
            var attribute1 = new BinaryDB.Field(fieldType, attribute1Data);
            string recordExtId = "record1";
            List<Record> result = await db.WriteAsync(
                new BinaryDB.Record(new RecordId(recordExtId, type),
                    new List<BinaryDB.Field>() { attribute1 },
                    state: RecordState.Full));

            // 3. Reload the db and verify the record 
            db.Dispose();
            db = await BinaryDB.BinaryDB.LoadOrCreateDBAsync(DbName, Folder);
            var result2 = await db.Read(recordExtId, type);
            bool passed = false;
            bool isRecordIdIdentical = result2 != null && result.Count == 1 && result2.Id.Id == result[0].Id.Id;
            Assert.IsTrue(isRecordIdIdentical, "Saved record Id is not identical to retrieved Id");
            if(isRecordIdIdentical)
                Assert.IsTrue(passed = result2 != null && HasAttribute(result2, attribute1));
            if (passed)
                Assert.IsTrue(result2?.Id.Type == result[0].Id.Type);

            if(passed)
            {
                // 4. Add new attribute to record
                byte[] attribute2Data = new byte[] { 0x02, 0x03 };
                int fieldType2 = 3;
                var attribute2 = new BinaryDB.Field(fieldType2, attribute2Data);
                var result3 = await db.WriteAsync(new BinaryDB.Record(new RecordId(recordExtId, type),
                   new List<BinaryDB.Field>() { attribute2 },
                   state: BinaryDB.RecordState.Partial));
                db.Dispose();

                // 5. Reload the database and verify both attributes
                db = await BinaryDB.BinaryDB.LoadOrCreateDBAsync(DbName, Folder);
                result2 = await db.Read(recordExtId, type);
                Assert.IsTrue(passed = result2 != null && result.Count == 1 && result2.Id.Id == result3[0].Id.Id);
                if(passed)
                    Assert.IsTrue(passed = result2 != null && HasAttribute(result2, attribute1));
                if(passed)
                    Assert.IsTrue(passed = result2 != null && HasAttribute(result2, attribute2));
            }

            // 6. Delete db
            db.Dispose();
            BinaryDB.BinaryDB.DeleteDB(DbName, Folder);
        }

        [Test]
        public async Task RecordWithAttachmentAndDelete()
        {
            // 1. Create db from scratch
            BinaryDB.BinaryDB.DeleteDB(DbName, Folder);
            var db = await BinaryDB.BinaryDB.LoadOrCreateDBAsync(DbName, Folder);

            // 2. Write a record with a single attribute and attachment
            int recordType = 1;
            var attribute1 = new BinaryDB.Field(2, new byte[] { 0x01, 0x02 });
            var attribute2 = new BinaryDB.Field(3, new byte[] { 0x03, 0x04 });
            string recordExtId = "record1";
            Record attachment = new Record(new RecordId("attachment1", 2),
                new List<BinaryDB.Field>() { attribute2 },
                state: RecordState.Full);
            var attribute3 = new BinaryDB.Field(3, FieldState.Attachment, record: attachment);
            List<Record> result = await db.WriteAsync(
                new BinaryDB.Record(new RecordId(recordExtId, recordType),
                    new List<BinaryDB.Field>() { attribute1, attribute3 },
                    RecordState.Full));

            // 3. Reload the db and verify the record
            db.Dispose();
            db = await BinaryDB.BinaryDB.LoadOrCreateDBAsync(DbName, Folder);
            var result2 = await db.Read(recordExtId, recordType);
            bool passed = false;
            bool isRecordIdIdentical = result2 != null && result2.Id.Id == result[0].Id.Id;
            Assert.IsTrue(isRecordIdIdentical, "Saved record Id is not identical to retrieved Id");
            if (isRecordIdIdentical)
                Assert.IsTrue(passed = result2 != null && HasAttribute(result2, attribute1));
            if (passed)
                Assert.IsTrue(passed = result2?.Id.Type == result[0].Id.Type);


            // 6. Delete db
            db.Dispose();
            BinaryDB.BinaryDB.DeleteDB(DbName, Folder);
        }

        static bool HasAttribute(BinaryDB.Record record, BinaryDB.Field attribute)
        {
            var att = record.Attributes?.FirstOrDefault(a => a.Type == attribute.Type);

            if(att == null)
            {
                return false;
            }

            return att.Data != null && attribute.Data != null &&
                att.Data.SequenceEqual(attribute.Data);
        }
    }
}