using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using NUnit.Framework;

namespace cachey_bashi.tests
{
    [TestFixture]
    public class CacheyBashiTests
    {
        private IEnumerable<KeyValuePair<HashBin, byte[]>> GenerateJunkData(int count)
        {
            var r = new Random(DateTime.UtcNow.Millisecond);
            List<KeyValuePair<HashBin, byte[]>> junkData = new List<KeyValuePair<HashBin, byte[]>>();
            for (int i = 0; i < count; i++)
            {
                var buf = new byte[16];
                r.NextBytes(buf);
                yield return new KeyValuePair<HashBin, byte[]>(
                    buf.ToHashBin(false),
                    buf
                );
            }   
        }
        
        
        [Test]
        [TestCase(10000)]
        [TestCase(100000)]
        [TestCase(235000)]
        //[TestCase(1000000)]
        // [TestCase(3000000)]
        // [TestCase(10000000)]
        public void ReloadTest(int count)
        {
            var junkData = GenerateJunkData(count).ToList();
            
            var dir = Path.GetTempPath();
            var dbName = "cbunittest_reload";
            var sw = new Stopwatch();
            sw.Start();
            var cb = CacheyBashi.Create(dir, dbName, junkData, 16, 2);
            sw.Stop();
            
            Console.WriteLine($"Took: {sw.ElapsedMilliseconds:N} to create CacheBashi DB with {junkData.Count} key/value pairs");

            
            cb.Dispose();
            sw.Restart();
            cb = CacheyBashi.Load(dir, dbName, 16, 2);
            sw.Stop();
            
            Console.WriteLine($"Took: {sw.ElapsedMilliseconds:N} to load CacheBashi DB from disk with {junkData.Count} key/value pairs");
            
            sw.Reset();
            
            foreach (var junk in junkData)
            {
                sw.Start();
                var hasKey = cb.HasKey(junk.Key);
                sw.Stop();
                Assert.True(hasKey);
            }
            
            Console.WriteLine($"Took: {sw.ElapsedMilliseconds:N} to check if {junkData.Count:N} keys exist (where all keys exist)");

            sw.Reset();
            
            foreach (var junk in junkData)
            {
                sw.Start();
                var value = cb.GetValue(junk.Key);
                sw.Stop();
                Assert.AreEqual(value.ToHashBin(false), junk.Key);
            }
            
            Console.WriteLine($"Took: {sw.ElapsedMilliseconds:N} to fetch {junkData.Count:N} values (where all keys exist)");
            Console.WriteLine($"out dir: {dir}");
        }

        [Test]
        public void LostUpdateTest()
        {
            var dir = Path.GetTempPath();
            var dbName = "cbunittest_lostupdate";
            var sw = new Stopwatch();
            var junkdata = GenerateJunkData(1000).ToList();

            
            Console.WriteLine($"adding multiple items with key: {junkdata[0].Key.Hash.ToHexString()}");
            for (int i = 0; i < 1000; i++)
            {
                junkdata.Add(new KeyValuePair<HashBin, byte[]>(
                    junkdata[0].Key,
                    Encoding.UTF8.GetBytes($"fail {i}")
                ));    
            }
            
            junkdata.Add(new KeyValuePair<HashBin, byte[]>(
                junkdata[0].Key,
                Encoding.UTF8.GetBytes("success")
            ));    
            
            
            using var cb = CacheyBashi.Create(dir, dbName, junkdata, 16);

            var value = Encoding.UTF8.GetString(cb.GetValue(junkdata[0].Key));
            
            Assert.AreEqual("success", value);
        }
    }

}