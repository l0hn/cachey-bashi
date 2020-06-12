using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using NUnit.Framework;

namespace cachey_bashi.tests
{
    [TestFixture]
    public class CacheyBashiTests
    {
        [Test]
        public void ReloadTest()
        {
            var r = new Random(DateTime.UtcNow.Millisecond);
            List<KeyValuePair<HashBin, byte[]>> junkData = new List<KeyValuePair<HashBin, byte[]>>();
            for (int i = 0; i < 200000; i++)
            {
                var buf = new byte[16];
                r.NextBytes(buf);
                junkData.Add(new KeyValuePair<HashBin, byte[]>(
                        buf.ToHashBin(false),
                        buf
                    ));
            }    
            
            var dir = Path.GetTempPath();
            var dbName = "cbunittest_reload";
            var sw = new Stopwatch();
            sw.Start();
            var cb = CacheyBashi.Create(dir, dbName, junkData, 16);
            sw.Stop();
            
            Console.WriteLine($"Took: {sw.ElapsedMilliseconds:N} to create CacheBachi DB with {junkData.Count} key/value pairs");

            cb.Dispose();
            sw.Start();
            cb = CacheyBashi.Load(dir, dbName, 16);
            sw.Stop();
            
            Console.WriteLine($"Took: {sw.ElapsedMilliseconds:N} to load CacheBachi DB from disk with {junkData.Count} key/value pairs");
            
            foreach (var junk in junkData)
            {
                sw.Start();
                var hasKey = cb.HasKey(junk.Key);
                sw.Stop();
            }
            
            Console.WriteLine($"Took: {sw.ElapsedMilliseconds:N} to check if {junkData.Count:N} keys exist (where all keys exist)");

            foreach (var junk in junkData)
            {
                sw.Start();
                var value = cb.GetValue(junk.Key);
                sw.Stop();
            }
            
            Console.WriteLine($"Took: {sw.ElapsedMilliseconds:N} to fetch {junkData.Count:N} values (where all keys exist)");
        }
    }

}