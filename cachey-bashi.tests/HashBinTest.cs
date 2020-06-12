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
        [TestCase(16, true)]
        [TestCase(32, true)]
        [TestCase(64, true)]
        [TestCase(65, true)]
        [TestCase(17, true)]//will cause padding in HashBin backing array.
        [TestCase(14, true)]//will cause padding in HashBin backing array.
        [TestCase(6, true)]
        [TestCase(16, false)]
        [TestCase(32, false)]
        [TestCase(64, false)]
        [TestCase(65, false)]
        [TestCase(17, false)]//will cause padding in HashBin backing array.
        [TestCase(14, false)]//will cause padding in HashBin backing array.
        [TestCase(6, false)]
        public void EqualityBasic(int keyLen, bool hashBinCopy)
        {
            var r = new Random(DateTime.UtcNow.Millisecond);
            var arrA = new byte[keyLen];
            var arrC = new byte[keyLen];
            r.NextBytes(arrA);
            r.NextBytes(arrC);
            
            var hashA = new HashBin(arrA, hashBinCopy);
            var hashB = new HashBin(arrA, hashBinCopy);

            Assert.True(hashA == hashB);
            
            var hashC = new HashBin(arrC, hashBinCopy);
            Assert.False(hashA == hashC);
        }

        [Test]
        [TestCase("ffffffffffffffff", "fffffffffffffffe", false)]
        [TestCase("fffffffffffffffe", "ffffffffffffffff", true)]
        [TestCase("0fffffffffffffff", "1fffffffffffffff", true)]
        [TestCase("1ffffffffffffffe", "1fffffffffffffff", true)]
        public void ComparisonTest(string a, string b, bool bIsGreater)
        {
            var hashBinA = new HashBin(a);
            var hashBinB = new HashBin(b);
            if (bIsGreater)
            {
                Assert.True(hashBinA < hashBinB);
            }
            else
            {
                Assert.False(hashBinA < hashBinB);
            }
        }
        

        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public void RandTest(bool hashBinCopy)
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
            
            Console.WriteLine($"Creating {count:N0} HashBins");
            for (int i = 0; i < count; i++)
            {
                sw.Start();
                hashBins[i] = new HashBin(randHashes[i], hashBinCopy);
                sw.Stop();
            }
            Console.WriteLine($"Took {sw.ElapsedMilliseconds:N2}ms to create {count:N0} HashBins");
            sw.Reset();
            Console.WriteLine($"Creating {count:N0} Hash Strings");
            for (int i = 0; i < count; i++)
            {
                sw.Start();
                hashStrs[i] = randHashes[i].ToHexString();
                sw.Stop();
            }
            Console.WriteLine($"Took {sw.ElapsedMilliseconds:N2}ms to create {count:N0} Hash Strings");
            
            sw.Reset();
            Console.WriteLine($"Comparing {count:N0} HashBins");
            int matches = 0;
            for (int i = 0; i < count; i++)
            {
                sw.Start();
                if (new HashBin(randHashes[i], hashBinCopy) == hashBins[i])
                    matches++;
                sw.Stop();
            }
            Console.WriteLine($"Took {sw.ElapsedMilliseconds:N2}ms to compare {count:N0} HashBins (matches={matches:N0})");
            Assert.AreEqual(count, matches);
            sw.Reset();
            Console.WriteLine($"Comparing {count:N0} HashStrings");
            matches = 0;
            for (int i = 0; i < count; i++)
            {
                sw.Start();
                if (randHashes[i].ToHexString() == hashStrs[i])
                    matches++;
                sw.Stop();
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