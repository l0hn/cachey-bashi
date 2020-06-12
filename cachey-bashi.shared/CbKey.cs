using System;
using System.Diagnostics;
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
        private byte[] _readBuffer;
        private HashBin _readBinBuffer;
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

            FileStream = File.Open(_file, FileMode.OpenOrCreate);
            _reader = new BinaryReader(FileStream);
            _readBuffer = new byte[_keyLength*10000];
            _readBinBuffer = new HashBin(new byte[_keyLength], false);

            if (FileStream.Length > 8)
            {
                _count = _reader.ReadUInt64();
                _keyEnd = _count * _keyLength;
            }
        }

        internal void PostWriteUpdate()
        {
            if (FileStream.Length > 8)
            {
                FileStream.Position = 0;
                _count = _reader.ReadUInt64();
                _keyEnd = _count * _keyLength;
            }
        }

        public bool HasKey(byte[] key, KeyHint hint = default(KeyHint))
        {
            return HasKey(new HashBin(key, false), hint);
        }

        public bool HasKey(HashBin key, KeyHint hint = default(KeyHint))
        {
            return HasKey(key, out var unused, false, hint);
        }

        public bool GetKeyDataAddr(HashBin key, out DataAddr dataAddr, KeyHint hint = default(KeyHint))
        {
            return HasKey(key, out dataAddr, true, hint);
        }


        public bool HasKey(HashBin key, out DataAddr dataAddr, bool getDataAddr, KeyHint hint = default(KeyHint))
        {
            return HasKeyFast(key, out dataAddr, getDataAddr, hint);
        }
        
        public bool HasKeySlow(HashBin key, out DataAddr dataAddr, bool getDataAddr = false, KeyHint hint = default(KeyHint))
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

            //This is horrifically slow: need to load chunks of data into memory and read from there instead
            while (FileStream.Position <= (long)hint.EndAddr)
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
        
        public bool HasKeyFast(HashBin key, out DataAddr dataAddr, bool getDataAddr = false, KeyHint hint = default(KeyHint))
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
            
            var rangeSize = hint.EndAddr - hint.StartAddr;
            var remaining = (long)rangeSize+_keyLength;
            long lastRead;
            int amountToRead;
            var bufReadPos = 0;
            while (remaining > 0) 
            {
                amountToRead = (int)(remaining < _readBuffer.Length ? remaining : _readBuffer.Length);
                lastRead = FileStream.Read(_readBuffer, 0, amountToRead);
                remaining -= lastRead;
                bufReadPos = 0;
                //loop until buffer read
                while (bufReadPos < lastRead)
                {
                    _readBinBuffer.SetFromPartialArray(_readBuffer, bufReadPos, _keyLength, false);

                    if (_readBinBuffer == key)
                    {
                        if (getDataAddr)
                        {
                            var foundLocation = FileStream.Position - lastRead + bufReadPos;
                            FileStream.Position = ((((foundLocation) / _keyLength) << 4) + (long)_keyEnd + (long)HeaderLength);
                            dataAddr.addr = _reader.ReadUInt64();
                            dataAddr.len = _reader.ReadUInt64();
                        }

                        return true;
                    }
                    bufReadPos += _keyLength;
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