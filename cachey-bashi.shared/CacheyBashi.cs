using System;
using System.Collections.Generic;
using System.IO;

namespace cachey_bashi
{
    public class CacheyBashi<T>
    {
        private string _dir;
        private string _dbName;

        private string _indexFile;
        private string _keyFile;
        private string _datFile;

        private CbIndex _cbIndex;
        
        public CacheyBashi(string directory, string dbName)
        {
            //todo: load the index.cb files into memory
            _dir = directory;
            _dbName = dbName;
            _indexFile = Path.Combine(_dir, dbName) + ".index";
            _keyFile = Path.Combine(_dir, dbName) + ".key";
            _datFile = Path.Combine(_dir, dbName) + ".dat";
            _cbIndex = new CbIndex(_indexFile);
        }

        public byte[] GetValue(byte[] key)
        {
            //read in-mem index to get addr of first 4bytes of key in key file (starting point for keys beginning with e.g. 0x1234)
            //possibly want the last index of the e.g 0x1234 so we don't need to continually compare the first 4 bytes.
            
            //start reading from the key file a ulong at a time, use bitwise to compare  
            return null;
        }
        
        // public void Insert(byte[] key, byte[] data)
        // {
        //     //todo: not sure how to do this one, will need some sort of buffer of newly added values followed by a 'compaction' of sorts
        //     //not really the point of this project's intended purpose but might be useful in future.
        // }

        public void Write(IEnumerable<KeyValuePair<byte[], byte[]>> data)
        {
            var indexFile = $"{_indexFile}.tmp";
            var datFile = $"{_indexFile}.tmp";
            var keyFile = $"{_indexFile}.tmp";
            //todo: Write the index, key, and data files.  
            
            //take batches of 100k? arbitraty or maybe roughly calc mem requirements
            var keyDataArray = new KeyData[100000];
            var index = 0;
            var datFileIndex = 0;

            foreach (var kvp in data)
            {
                keyDataArray[index++].Key=kvp.Key;
                keyDataArray[index].DataFileAddress = datFileIndex;
                keyDataArray[index].DataLength = kvp.Value.Length;
                //need to write the dat file here so we can discard data from memory
                datFileIndex += kvp.Value.Length;
            }
        }
    }

    struct KeyData
    {
        public byte[] Key;
        public long DataFileAddress;
        public long DataLength;
    }
}