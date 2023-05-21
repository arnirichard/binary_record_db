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
        public async Task Test1()
        {
            var db = await BinaryDB.BinaryDB.LoadOrCreateAsync("test", @"C:\Temp\DBTEST");
            byte[] data = new byte[] { 0x01, 0x02 };

             var result = await db.WriteAsync(new BinaryDB.Record("test1",
                new List<BinaryDB.Attribute>() { new BinaryDB.Attribute("Field1", data) },
                null,
                state: BinaryDB.RecordState.Full));

            db.Dispose();

            db = await BinaryDB.BinaryDB.LoadOrCreateAsync("test", @"C:\Temp\DBTEST");

            var result2 = await db.Read("test1");

            Assert.IsTrue(result2?.Attributes?.Count == 1 == true);

            var attribute = result2?.Attributes?.FirstOrDefault();

            if(attribute != null)
            {
                Assert.IsTrue(attribute.Data?.Length == 2);
                if(attribute.Data?.Length == 2)
                {
                    Assert.IsTrue(data.SequenceEqual(attribute.Data));
                }
            }
        }
    }
}