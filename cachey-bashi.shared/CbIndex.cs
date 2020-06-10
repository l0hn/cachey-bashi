using System;
using System.IO;

namespace cachey_bashi
{
    public class CbIndex
    {
        //data format
        //0x00: 1 byte indicating the index key size
        //0x01..n: 8 bytes indicating memory address of the beginning of the keys in the key file for this address offset
        //e.g. 0x9998(+1) contains the start address for keys beginning with 0x9998 in the key file 
        private byte[] _indexData;
        
        private string _file;
        private byte _indexKeyLen;

        private int _requiredDatLength;

        private static readonly int _ulongSize = sizeof(ulong);

        private readonly int _keyShift;
        
        public CbIndex(string indexFile, byte indexKeyLen = 2, bool overwriteNew = false)
        {
            if (indexKeyLen > sizeof(ulong))
                throw new ArgumentException($"Index key length cannot be larger than {sizeof(ulong)}");
            
            _file = indexFile;
            _indexKeyLen = indexKeyLen;
            _requiredDatLength = CalculateRequiredLength();
            _indexData = new byte[_requiredDatLength];
            _indexData[0] = _indexKeyLen;
            _keyShift = (_ulongSize - (_indexKeyLen))*8;
            if (overwriteNew)
            {
                File.Delete(indexFile);
            }
            else
            {
                LoadFromDisk();    
            }
        }

        int CalculateRequiredLength()
        {
            //max number representable by index key byte size
            var maxAddrCount = (long)Math.Pow(2, _indexKeyLen * 8);
            return (int)(maxAddrCount * sizeof(ulong)) + 1;
        }
        
        public void WriteToDisk()
        {
            File.WriteAllBytes(_file, _indexData);
        }

        public void LoadFromDisk()
        {
            if (!File.Exists(_file))
                return;
            
            //todo: read the file
            using var file = File.Open(_file, FileMode.Open);
            using var reader = new BinaryReader(file);
            var indexKeyLen = reader.ReadByte();
            if (indexKeyLen != _indexKeyLen)
            {
                throw new ArgumentException($"Index key length error: expected {_indexKeyLen}, however index file has {indexKeyLen}");
            }

            if (file.Length != _requiredDatLength)
            {
                throw new ArgumentException($"Index file length is incorrect, expected {_requiredDatLength}, got {file.Length}");
            }

            file.Position = 0;
            reader.Read(_indexData, 0, _requiredDatLength);
        }

        public unsafe void SetStartIndexForKey(byte[] key, ulong addrStart)
        {
            var index = GetKeyIndexFromKey(key);
            fixed (byte* pIndex = &_indexData[index])
            {
                (*(ulong*) pIndex) = addrStart;
            }
        }

        unsafe int GetKeyIndexFromKey(byte[] key)
        {
            if (key.Length < _indexKeyLen)
            {
                throw new ArgumentException("key must be larger than the key index");
            }
            
            int indexLocation = -1; 
            if (key.Length >= _ulongSize)
            {
                fixed (byte* pKey = &key[^(_ulongSize)])
                {
                    indexLocation = (int)(((*(ulong*) pKey) >>_keyShift)<<3)+1;
                }
            }
            else
            {
                for (int i = _indexKeyLen-1; i > 0; i--)
                {
                    indexLocation += key[i] << ((i - _indexKeyLen)<<3);
                }
            }

            return indexLocation;
        }
        
        public unsafe ulong GetStartAddressForKey(byte[] key)
        {
            var indexLocation = GetKeyIndexFromKey(key);
            if (indexLocation > 0)
            {
                fixed (byte* pIndex = &_indexData[indexLocation])
                {
                    return *(ulong*)pIndex;
                }
            }
            return 0;
        }
        
        // private void TypeCheck()
        // {
        //     if (!_validTypes.Contains(typeof(T)))
        //     {
        //         throw new ArgumentException("Type of T must one of: " + string.Concat(_validTypes, ", "));
        //     }
        // }
    }
}