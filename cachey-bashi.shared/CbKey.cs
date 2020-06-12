using System;
using System.IO;
using System.Net;

namespace cachey_bashi
{
    /// <summary>
    /// Responsible for managing access to the key file
    /// </summary>
    public class CbKeyFixed: IDisposable
    {
        private string _file;
        private ulong _count;
        private ushort _keyLength;
        internal FileStream FileStream { get; }
        private BinaryReader _reader;
        private ulong _keyStart;
        private ulong _keyEnd;
        private ulong _ulongSize;
        internal ulong HeaderLength { get; }

        public CbKeyFixed(string keyFile, ushort keyLength = 16, bool createNew = false)
        {
            _ulongSize = sizeof(ulong);
            HeaderLength = _ulongSize;
            _keyStart = HeaderLength;
            
            _keyLength = keyLength;
            _file = keyFile;
            if (File.Exists(_file) && createNew)
                File.Delete(_file);

            FileStream = File.OpenWrite(_file);
            _reader = new BinaryReader(FileStream);

            if (FileStream.Length > 8)
            {
                _count = _reader.ReadUInt64();
                _keyEnd = _count * _keyLength;
            }
        }

        public bool HasKey(HashBin key, KeyHint hint = default(KeyHint))
        {
            return HasKey(key, out var unused, false, hint);
        }

        public bool GetKeyDataAddr(HashBin key, out DataAddr dataAddr, KeyHint hint = default(KeyHint))
        {
            return HasKey(key, out dataAddr, true, hint);
        }
        
        public bool HasKey(HashBin key, out DataAddr dataAddr, bool getDataAddr = false, KeyHint hint = default(KeyHint))
        {
            dataAddr = new DataAddr();
            if (key.Length != _keyLength)
                throw new ArgumentException($"Key must be {_keyLength} bytes");

            bool hasHint = hint.StartAddr >= _keyStart && 
                           hint.EndAddr > hint.StartAddr 
                           && hint.EndAddr <= _keyEnd;

            if (!hasHint)
            {
                hint.StartAddr = _keyStart;
                hint.EndAddr = _keyEnd;
            }
            
            FileStream.Position = (long)hint.StartAddr;
            while (FileStream.Position < (long)hint.EndAddr)
            {
                var currentHashBin = new HashBin(FileStream, _keyLength);
                if (currentHashBin == key)
                {
                    if (getDataAddr)
                    {
                        var foundLocation = (ulong)FileStream.Position - _keyLength;
                        FileStream.Position = (long)((((foundLocation) / _keyLength) << 4) + _keyEnd + HeaderLength);
                        dataAddr.addr = _reader.ReadUInt64();
                        dataAddr.len = _reader.ReadUInt64();
                    }
                    return true;
                }
            }
            return false;
        }

        public void Dispose()
        {
            FileStream?.Dispose();
        }
    }

    public struct DataAddr
    {
        public ulong addr;
        public ulong len;
    }
}