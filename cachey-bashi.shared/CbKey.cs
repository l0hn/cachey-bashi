using System;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Runtime.InteropServices;

namespace cachey_bashi
{
    /// <summary>
    /// Responsible for managing access to the key file
    /// </summary>
    public unsafe class CbKeyFixed: IDisposable
    {
        private string _file;
        private ulong _count;
        private ushort _keyLength;
        internal FileStream FileStream { get; }
        internal FileStream AddrsFileStream { get; }
        private string addrFile => _file + ".addrs";
        private BinaryReader _addrReader;
        private BinaryReader _reader;
        private ulong _keyStart;
        private ulong _keyEnd;
        private ulong _ulongSize;
        private byte[] _readBuffer;
        private HashBin _readBinBuffer;
        private GCHandle _readBufferHandle;
        private byte* _readBufferPtr;
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

            if (File.Exists(addrFile) && createNew)
                File.Delete(addrFile);
            
            FileStream = File.Open(_file, FileMode.OpenOrCreate);
            AddrsFileStream = File.Open(addrFile, FileMode.OpenOrCreate);
            _reader = new BinaryReader(FileStream);
            _readBuffer = new byte[_keyLength*10000];
            _readBufferHandle = GCHandle.Alloc(_readBuffer, GCHandleType.Pinned);
            _readBufferPtr = (byte*) _readBufferHandle.AddrOfPinnedObject();
            _readBinBuffer = new HashBin(new byte[_keyLength], false);
            _readBinBuffer.SetFromPartialArray(_readBuffer, 0, _keyLength, false);

            _addrReader = new BinaryReader(AddrsFileStream);
            
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

        public bool GetKeyDataAddr(HashBin key, out DataAddr dataAddr, KeyHint hint = default(KeyHint), byte skipBytes = 0)
        {
            return HasKey(key, out dataAddr, true, hint, skipBytes);
        }

        public bool HasKey(HashBin key, out DataAddr dataAddr, bool getDataAddr = false, KeyHint hint = default(KeyHint), byte skipBytes = 0)
        {
            if (key.Length != _keyLength)
                throw new ArgumentException($"Key must be {_keyLength} bytes");

            // bool hasHint = hint.StartAddr >= _keyStart && 
            //                hint.EndAddr >= hint.StartAddr 
            //                && hint.EndAddr <= _keyEnd;

            int compareLength = _keyLength-skipBytes;
            
            if (!(hint.StartAddr >= _keyStart && 
                  hint.EndAddr >= hint.StartAddr 
                  && hint.EndAddr <= _keyEnd))
            {
                hint.StartAddr = _keyStart;
                hint.EndAddr = _keyEnd;
                compareLength = _keyLength;
            }

            FileStream.Position = (long)hint.StartAddr;
            var remaining = (long)(hint.EndAddr - hint.StartAddr +_keyLength);
            long lastRead;
            int amountToRead;
            var bufReadPos = 0;
            int compareRes;
            fixed (byte* ptrKey = &key.Hash[0])
            {
                while (remaining > 0) 
                {
                    amountToRead = (int)(remaining < _readBuffer.Length ? remaining : _readBuffer.Length);
                    lastRead = FileStream.Read(_readBuffer, 0, amountToRead);
                    remaining -= lastRead;
                    bufReadPos = 0;
                    //loop until buffer read
                    while (bufReadPos < lastRead)
                    {
                        //_readBinBuffer.SetPointer(_readBufferPtr+bufReadPos, _keyLength);
                        //compareRes = _readBinBuffer.CompareTo(key);
                        compareRes = HashBin.ArrayPtrCompare(_readBufferPtr + bufReadPos, ptrKey, compareLength);

                        if (compareRes == 0)
                        {
                            if (getDataAddr)
                            {
                                var foundLocation = FileStream.Position - lastRead + bufReadPos;
                                //FileStream.Position = ((((foundLocation) / _keyLength) << 4) + (long)_keyEnd + (long)HeaderLength);
                                AddrsFileStream.Position = (foundLocation / _keyLength) << 4;
                                dataAddr = new DataAddr
                                {
                                    addr = _addrReader.ReadUInt64(), 
                                    len = _addrReader.ReadUInt64()
                                };
                                return true;
                            }

                            dataAddr = default;
                            return true;
                        }

                        if (compareRes == 1)
                        {
                            dataAddr = default;
                            return false;
                        }
                        //
                        // if (HashBin.ArrayPtrEqualCompare(ptrKey, _readBufferPtr+bufReadPos, compareLength))
                        // {
                        //     if (getDataAddr)
                        //     {
                        //         var foundLocation = FileStream.Position - lastRead + bufReadPos;
                        //         FileStream.Position = ((((foundLocation) / _keyLength) << 4) + (long)_keyEnd + (long)HeaderLength);
                        //         dataAddr = new DataAddr();
                        //         dataAddr.addr = _reader.ReadUInt64();
                        //         dataAddr.len = _reader.ReadUInt64();
                        //         return true;
                        //     }
                        //
                        //     dataAddr = default;
                        //     return true;
                        // }

                        
                        

                        bufReadPos += _keyLength;
                    }
                
                }
            }

            dataAddr = default;
            return false;
        }

        public void Dispose()
        {
            FileStream?.Dispose();
            AddrsFileStream?.Dispose();
        }
    }

    public struct DataAddr
    {
        public ulong addr;
        public ulong len;
    }
}