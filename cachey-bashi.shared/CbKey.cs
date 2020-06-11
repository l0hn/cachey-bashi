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
        private FileStream _fileStream;
        private BinaryReader _reader;
        private ulong _keyStart = sizeof(ulong);
        private ulong _keyEnd;

        public CbKeyFixed(string keyFile, byte keyLength = 16, bool createNew = false)
        {
            _keyLength = keyLength;
            _file = keyFile;
            if (File.Exists(_file) && createNew)
                File.Delete(_file);

            _fileStream = File.OpenWrite(_file);
            _reader = new BinaryReader(_fileStream);

            if (_fileStream.Length > 8)
            {
                _count = _reader.ReadUInt64();
                _keyEnd = _count * _keyLength;
            }
        }

        public bool GetKeyDataAddrLocation(HashBin key, out ulong location, ulong startHint = 0, ulong endHint = 0)
        {
            location = 0;
            if (key.Length != _keyLength)
                throw new ArgumentException($"Key must be {_keyLength} bytes");

            bool hasHint = startHint >= _keyStart && 
                           endHint > startHint 
                           && endHint <= _keyEnd;

            if (!hasHint)
            {
                startHint = _keyStart;
                endHint = _keyEnd;
            }
            
            _fileStream.Position = (long)startHint;
            for (ulong position = startHint; position < endHint; position += _keyLength)
            {
                var currentHashBin = new HashBin(_fileStream, _keyLength);
                if (currentHashBin == key)
                {
                    var foundLocation = position - _keyLength;
                    //how can I avoid this division? 
                    location = _keyStart + (((foundLocation - _keyStart) / _keyLength) << 4);
                    return true;
                }
            }
            
            return false;
        }

        public void Dispose()
        {
            _fileStream?.Dispose();
        }
    }
}