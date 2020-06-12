using System;
using System.Diagnostics;
using System.IO;
using NUnit.Framework;

namespace cachey_bashi.tests
{
    /*
    These are not tests of Cachey-Bashi, just some tests to help decide if seqential read + calculation to seek to 
    addr is faster than aligning addresses with keys. It seems sequential read + calc always 'wins'
    */
    [TestFixture]
    public class ReadSpeedTest
    {
        string GenerateDummyFile(long length)
        {
            var tmpFile = Path.GetTempFileName();
            var r = new Random(DateTime.UtcNow.Millisecond);
            var buf = new byte[1024];
            var written = 0;
            using var f = File.OpenWrite(tmpFile);
            
            while (written < length)
            {
                r.NextBytes(buf);
                if (written + buf.Length > length)
                {
                    var remainder = (int) length - written;
                    f.Write(buf, 0, remainder);
                    written += remainder;
                }
                else
                {
                    f.Write(buf, 0, buf.Length);
                    written += buf.Length;
                }
            }
            return tmpFile;
        }
        
        [Test]
        [TestCase(1)]
        [TestCase(100)]
        [TestCase(1000)]
        [TestCase(10000)]
        [TestCase(100000)]
        [TestCase(1000000)]
        public void ReadSequential(long keyCount)
        {
            var keyLength = 16;
            var keyEnd = keyCount * 16;
            var file = GenerateDummyFile(keyCount*16*2);
            //typical usage scenario is to read keys of 16bytes at a time
            using var f = File.OpenRead(file);
            var reader = new BinaryReader(f);
            long count = 0;
            Stopwatch sw = new Stopwatch();
            while (count < keyCount)
            {
                sw.Start();
                HashBin hb = new HashBin(f, 16);
                sw.Stop();
                count++;
            }
            //need to simulate that we've found the key
            sw.Start();
            f.Position = (((f.Position-keyLength) / keyLength) << 4) + keyEnd;
            var addr = reader.ReadUInt64();
            var len = reader.ReadUInt64();
            sw.Stop();
            
            Console.WriteLine($"Sequential read for {count:N0} items took {sw.ElapsedMilliseconds:N10}ms");
            File.Delete(file);
        }

        [TestCase(1)]
        [TestCase(100)]
        [TestCase(1000)]
        [TestCase(10000)]
        [TestCase(100000)]
        [TestCase(1000000)]
        public void ReadNonSequential(long keyCount)
        {
            var file = GenerateDummyFile(keyCount*16*2);
            
            //typical usage scenario is to read keys of 16bytes at a time
            using var f = File.OpenRead(file);
            var reader = new BinaryReader(f);
            long count = 0;
            Stopwatch sw = new Stopwatch();
            while (count < keyCount)
            {
                sw.Start();
                HashBin hb = new HashBin(f, 16);//offset here is simulating skipping the addr and length [16b key][8b address][8b len]
                f.Seek(16, SeekOrigin.Current);
                sw.Stop();
                count++;
            }
 
            //rewind the position (we wouldn't actually need to do this so don't time it)
            f.Position -= 16;
            
            sw.Start();
            var addr = reader.ReadUInt64();//we're already positioned to read the addr and len
            var len = reader.ReadUInt64();
            sw.Stop();
            
            Console.WriteLine($"Non-Sequential read for {count:N0} items took {sw.ElapsedMilliseconds:N10}ms");
            File.Delete(file);
        }
    }
}