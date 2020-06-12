using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Newtonsoft.Json;
using NUnit.Framework;

namespace cachey_bashi.tests
{
    [TestFixture]
    public class CbKeyTest
    {
        IEnumerable<KeyValuePair<byte[], byte[]>> GenerateDummyData(ushort keyLength, byte indexKeyLength)
        {
            //create some data that we can repeat later for verification
            //we need multiple items in each key space (index key length)
            //e.g.
            //0x0000ffffffffffffffffffffffffffff, 0x0001ffffffffffffffffffffffffffff .. etc
            //0x00000000000000000000000000000000, 0x00010000000000000000000000000000 .. etc
            //0x0000aaaaaaaaaaaaaaaaaaaaaaaaaaaa, 0x0001aaaaaaaaaaaaaaaaaaaaaaaaaaaa .. etc
            //0x00009999999999999999999999999999, 0x00019999999999999999999999999999 .. etc

            var numIndexes = CbIndex.MaxIndexesForIndexLength(indexKeyLength);

            string keyspaces = "0123456789abcdef";
            var indexOffset = keyLength - indexKeyLength;
            // var debugHashBin = new HashBin("0000000000000000000000000000d8f4");
            foreach (var keyspace in keyspaces)
            {
                var indexBuf = new byte[indexKeyLength];
                var pattern = new HashBin("".PadLeft(32, keyspace));
                for (int i = 0; i < numIndexes; i++)
                {
                    //set index on key
                    Array.Copy(indexBuf, 0, pattern.Hash, pattern.Hash.Length-indexBuf.Length, indexBuf.Length);
                    //
                    // if (pattern == debugHashBin)
                    // {
                    //     Console.WriteLine("DebugMe");
                    // }
                    
                    //send it
                    var dummyDat = new DummyData("This is your dummy data", indexBuf, pattern.Hash);
                    yield return new KeyValuePair<byte[], byte[]>(pattern.Hash, dummyDat.ToJsonBytes());
                    
                    //increment index bytes
                    IncrementBuf(indexBuf);
                }
            }
        }

        void IncrementBuf(byte[] buf)
        {
            for (int i = 0; i < buf.Length; i++)
            {
                if (buf[i] == 255)
                {
                    buf[i] = 0;
                }
                else
                {
                    buf[i]++;
                    return;
                }
            }
        }
        
        [Test]
        public void TestCbKey()
        {
            var dir = Path.GetTempPath();
            var dbName = "cbunittest";
            var cb = CacheyBashi.Create(dir, dbName, GenerateDummyData(16, 2), 16, 2);
            var debugSearch = new HashBin("0000000000000000000000000000d8f4");
            //now regen the dummy data to verify it all exits
            foreach (var kvp in GenerateDummyData(16, 2))
            {
                HashBin key = new HashBin(kvp.Key);
                var dummyData = DummyData.FromJsonBytes(kvp.Value);
                // if (key == debugSearch)
                // {
                //     Console.WriteLine("debug me");
                // }
                Assert.True(cb.HasKey(key), $"key not found: {key.Hash.ToHexString()}");
                //I haven't done the dat file yet :/
                // var storedDummyData = DummyData.FromJsonBytes(cb.GetValue(key));
                // Assert.AreEqual(dummyData.Message, storedDummyData.Message);
                // Assert.AreEqual(dummyData.OriginalKey.ToHashBin(false), storedDummyData.OriginalKey.ToHashBin(false));
                // Assert.AreEqual(dummyData.OriginalKeyIndex.ToHashBin(false), storedDummyData.OriginalKeyIndex.ToHashBin(false));
            }
        }
    }

    class DummyData
    {
        public string Message { get; set; }
        public byte[] OriginalKeyIndex { get; set; }
        public byte[] OriginalKey { get; set; }

        public DummyData(string message, byte[] originalKeyIndex, byte[] originalKey)
        {
            Message = message;
            OriginalKeyIndex = originalKeyIndex;
            OriginalKey = originalKey;
        }

        public string ToJson()
        {
            return JsonConvert.SerializeObject(this);
        }

        public static DummyData FromJson(string json)
        {
            return JsonConvert.DeserializeObject<DummyData>(json);
        }

        public static DummyData FromJsonBytes(byte[] jsonBytes)
        {
            if (jsonBytes == null)
                return null;
            
            return FromJson(Encoding.UTF8.GetString(jsonBytes));
        }
        
        public byte[] ToJsonBytes()
        {
            return Encoding.UTF8.GetBytes(ToJson());
        }
    }
}