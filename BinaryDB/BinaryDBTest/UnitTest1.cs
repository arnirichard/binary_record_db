using System.Diagnostics;

namespace BinaryDBTest
{
    public class Tests
    {
        [SetUp]
        public void Setup()
        {
        }

        [Test]
        public async Task CreateDbAndWriteAttributes()
        {
            string dbName = "test";
            string testFolder = @"C:\Temp\DBTEST";
            BinaryDB.BinaryDB.DeleteDB(dbName, testFolder);

            var db = await BinaryDB.BinaryDB.LoadOrCreateDBAsync(dbName, testFolder);
            byte[] data = new byte[] { 0x01, 0x02 };
            var attribute1 = new BinaryDB.Attribute("Field1", data);
            var result = await db.WriteAsync(new BinaryDB.Record("test1",
                new List<BinaryDB.Attribute>() { attribute1 },
                null,
                state: BinaryDB.RecordState.Full));

            db.Dispose();

            db = await BinaryDB.BinaryDB.LoadOrCreateDBAsync("test", @"C:\Temp\DBTEST");

            var result2 = await db.Read("test1");

            bool passed;

            Assert.IsTrue(passed = result2 != null && result.Count == 1 && result2.Id == result[0].Id);
            if(passed)
                Assert.IsTrue(passed = result2 != null && HasAttribute(result2, attribute1));

            if(passed)
            {
                byte[] data2 = new byte[] { 0x02, 0x03 };
                var attribute2 = new BinaryDB.Attribute("Field2", data2);
                var result3 = await db.WriteAsync(new BinaryDB.Record("test1",
                   new List<BinaryDB.Attribute>() { attribute2 },
                   null,
                   state: BinaryDB.RecordState.Partial));

                db.Dispose();

                db = await BinaryDB.BinaryDB.LoadOrCreateDBAsync("test", @"C:\Temp\DBTEST");

                result2 = await db.Read("test1");


                Assert.IsTrue(passed = result2 != null && result.Count == 1 && result2.Id == result3[0].Id);
                if(passed)
                    Assert.IsTrue(passed = result2 != null && HasAttribute(result2, attribute1));
                if(passed)
                    Assert.IsTrue(passed = result2 != null && HasAttribute(result2, attribute2));
            }

            db.Dispose();

            BinaryDB.BinaryDB.DeleteDB(dbName, testFolder);
        }

        static bool HasAttribute(BinaryDB.Record record, BinaryDB.Attribute attribute)
        {
            var att = record.Attributes?.FirstOrDefault(a => a.Id == attribute.Id);

            if(att == null)
            {
                return false;
            }

            return att.Data != null && attribute.Data != null &&
                att.Data.SequenceEqual(attribute.Data);
        }
    }
}