using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Security.Cryptography;
using NUnit.Framework;

namespace cachey_bashi.tests
{
    public class HashBinTest
    {
        [SetUp]
        public void Setup()
        {
        }

        [Test]
        [TestCase(16)]
        [TestCase(32)]
        [TestCase(64)]
        [TestCase(17)]//will cause padding in HashBin backing array.
        public void EqualityBasic(int keyLen)
        {
            var r = new Random(DateTime.UtcNow.Millisecond);
            var arrA = new byte[keyLen];
            var arrC = new byte[keyLen];
            r.NextBytes(arrA);
            r.NextBytes(arrC);
            
            var hashA = new HashBin(arrA);
            var hashB = new HashBin(arrA);

            Assert.True(hashA == hashB);
            
            var hashC = new HashBin(arrC);
            Assert.False(hashA == hashC);
        }

        [Test]
        public void RandTest()
        {
            int count = 100000;
            
            var randHashes = new List<byte[]>();
            var hashBins = new HashBin[count];
            var hashStrs = new string[count];
            
            var r = new Random(DateTime.UtcNow.Millisecond);
            
            Console.WriteLine($"Creating {count:N0} random hashes");
            
            for (int i = 0; i < count; i++)
            {
                var buf = new byte[r.Next(16, 256)];
                r.NextBytes(buf);
                randHashes.Add(buf.Md5());
            }

            var sw = new Stopwatch();
            sw.Start();
            
            Console.WriteLine($"Creating {count:N0} HashBins");
            for (int i = 0; i < count; i++)
            {
                hashBins[i] = new HashBin(randHashes[i]);
            }
            Console.WriteLine($"Took {sw.ElapsedMilliseconds:N2}ms to create {count:N0} HashBins");
            sw.Restart();
            Console.WriteLine($"Creating {count:N0} Hash Strings");
            for (int i = 0; i < count; i++)
            {
                hashStrs[i] = randHashes[i].ToHexString();
            }
            Console.WriteLine($"Took {sw.ElapsedMilliseconds:N2}ms to create {count:N0} Hash Strings");
            
            sw.Restart();
            Console.WriteLine($"Comparing {count:N0} HashBins");
            int matches = 0;
            for (int i = 0; i < count; i++)
            {
                if (new HashBin(randHashes[i]) == hashBins[i])
                    matches++;
            }
            Console.WriteLine($"Took {sw.ElapsedMilliseconds:N2}ms to compare {count:N0} HashBins (matches={matches:N0})");
            Assert.AreEqual(count, matches);
            sw.Restart();
            Console.WriteLine($"Comparing {count:N0} HashStrings");
            matches = 0;
            for (int i = 0; i < count; i++)
            {
                if (randHashes[i].ToHexString() == hashStrs[i])
                    matches++;
            }
            Console.WriteLine($"Took {sw.ElapsedMilliseconds:N2}ms to compare {count:N0} Hash Strings (matches={matches:N0})");
            Assert.AreEqual(count, matches);
        }

        [Test]
        [TestCase("e96c9661d2f7887a14264ee5986ea66d")]
        [TestCase("E96c9661d2f7887a14264ee5986ea66d")]
        public void StringConversion(string md5)
        {
            var hb = new HashBin(md5);
            Assert.AreEqual(md5.ToLower(), hb.ToString());
        }
    }
}