using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;

namespace cachey_bashi
{
    public class CacheyBashi: IDisposable
    {
        public string Dir { get; }
        public string DbName { get; }

        internal string IndexFile { get; }
        internal string KeyFile { get; }
        internal string DatFile { get; }

        internal CbIndex CbIndex { get; }
        internal CbKeyFixed CbKey { get; }
        
        internal CbData CbData { get; }

        private CacheyBashi(string directory, string dbName, ushort keyLength, byte indexKeyLength = 2, bool createNew = false)
        {
            //todo: load the index.cb files into memory
            Dir = directory;
            DbName = dbName;
            IndexFile = Path.Combine(Dir, dbName) + ".index";
            KeyFile = Path.Combine(Dir, dbName) + ".key";
            DatFile = Path.Combine(Dir, dbName) + ".dat";
            
            CbIndex = new CbIndex(IndexFile, indexKeyLength, createNew);
            CbKey = new CbKeyFixed(KeyFile, keyLength, createNew);
            CbData = new CbData(DatFile, createNew);    
        }

        public static CacheyBashi Create(string outDir, string dbName, IEnumerable<KeyValuePair<byte[], byte[]>> data, ushort keyLength, byte indexKeyLength = 2)
        {
            var cb = new CacheyBashi(outDir, dbName, keyLength, indexKeyLength, true);
            CbWriter.Write(cb, keyLength, data);

            return cb;
        }

        public static CacheyBashi Create(string outDir, 
            string dbName,
            IEnumerable<KeyValuePair<HashBin, byte[]>> data, 
            ushort keyLength, 
            byte indexKeyLength = 2)
        {
            var cb = new CacheyBashi(outDir, dbName, keyLength, indexKeyLength, true);
            CbWriter.Write(cb, keyLength, data);

            return cb;
        }

        public static CacheyBashi Load(string dir, string dbName, ushort keyLength)
        {
            var cb = new CacheyBashi(dir, dbName, keyLength);
            return cb;
        }

        public bool HasKey(byte[] key)
        {
            return HasKey(new HashBin(key, false));
        }

        public bool HasKey(HashBin key)
        {
            var hint = CbIndex.GetAddressHintForKey(key);
            return CbKey.HasKey(key, hint);
        }

        public bool IndexHasKeySpaceForKey(byte[] key)
        {
            var hint = CbIndex.GetAddressHintForKey(key);
            return hint.EndAddr > hint.StartAddr;
        }
        
        public byte[] GetValue(byte[] key)
        {
            return GetValue(new HashBin(key, false));
        }

        public byte[] GetValue(HashBin key)
        {
            var hint = CbIndex.GetAddressHintForKey(key);
            bool found = CbKey.GetKeyDataAddr(key, out var addr, hint);
            if (found)
            {
                return CbData.GetValue(addr);
            }
            return null;
        }

        public void Dispose()
        {
            CbKey?.Dispose();
            CbData?.Dispose();
        }
    }

    
}