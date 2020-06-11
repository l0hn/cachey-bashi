using System;
using System.Collections.Generic;
using System.Diagnostics;
using NUnit.Framework;

namespace cachey_bashi.tests
{
    [TestFixture]
    public class CbIndexTest
    {
        [Test]
        [TestCase(false, 2)]
        [TestCase(true, 2)]
        [TestCase(false, 3)]
        [TestCase(true, 3)]
        [TestCase(false, 4)]
        [TestCase(true, 4)]
        public void CbTest(bool shortHash = false, int indexKeyLength = 2)
        {
            int hashLength = shortHash ? 6 : 16;//16 bytes to simulate MD5 which is a common usage but could be any length key
            var cbIndex = new CbIndex("memtest.index", 2, true);
            var r = new Random(DateTime.UtcNow.Millisecond);

            Dictionary<ushort, byte[]> dummyData = new Dictionary<ushort, byte[]>();

            var maxKeys = (ushort)0;
            maxKeys = (ushort) ~maxKeys;
            
            for (ushort i = 0; i < maxKeys; i++)
            {
                var buf = new byte[hashLength];
                r.NextBytes(buf);
                //replace the first part of the key with sequential nums. e.g. 0x00, 0x01, 0x02
                //and leave the rest random nums to simulate real-world usage.
                buf[^1] = (byte) (i >> 8);
                buf[^2] = (byte) i;
                dummyData[i] = buf;
                cbIndex.SetStartIndexForKey(buf, (ulong)i+1);
            }
            
            //verify
            var verify = (Action<CbIndex>) ((c) =>
            {
                var sw = new Stopwatch();
            
                for (ushort i = 0; i < maxKeys; i++)
                {
                    var key = dummyData[i];
                    var expectedIndex = (ulong)i + 1;
                    sw.Start();
                    var res = c.GetStartAddressForKey(key);
                    sw.Stop();
                    Assert.AreEqual(expectedIndex, res);
                }
                Console.WriteLine($"Index lookup for {maxKeys} keys took: {sw.ElapsedMilliseconds:N}ms");
            });

            verify(cbIndex);
            
            //write it to file
            cbIndex.WriteToDisk();
            
            var cbLoadTest = new CbIndex("memtest.index", 2, false);

            verify(cbLoadTest);
        }
    }
}