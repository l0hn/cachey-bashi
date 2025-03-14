using System;
using System.IO;
using System.Runtime.InteropServices;

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
        internal byte IndexKeyLen { get; private set; }

        private int _requiredDatLength;

        private static readonly int _ulongSize = sizeof(ulong);

        private readonly int _keyShift;
        
        public CbIndex(string indexFile, byte indexKeyLen = 2, bool overwriteNew = false)
        {
            if (indexKeyLen > sizeof(ulong))
                throw new ArgumentException($"Index key length cannot be larger than {sizeof(ulong)}");
            
            _file = indexFile;
            IndexKeyLen = indexKeyLen;
            _requiredDatLength = CalculateRequiredLength();
            _indexData = new byte[_requiredDatLength];
            _indexData[0] = IndexKeyLen;
            _keyShift = (_ulongSize - (IndexKeyLen))*8;
            if (overwriteNew)
            {
                File.Delete(indexFile);
            }
            else
            {
                LoadFromDisk();    
            }
        }

        public static long MaxIndexesForIndexLength(byte indexLength)
        {
            return (long)Math.Pow(2, indexLength * 8);
        }
        
        int CalculateRequiredLength()
        {
            //max number representable by index key byte size
            var maxAddrCount = MaxIndexesForIndexLength(IndexKeyLen);
            return (int)(maxAddrCount * (sizeof(ulong)*2)) + 1;
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
            IndexKeyLen = reader.ReadByte();
            _requiredDatLength = CalculateRequiredLength();
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

        public unsafe void SetEndIndexForKey(byte[] key, ulong endAddr)
        {
            var index = GetKeyIndexFromKey(key);
            fixed (byte* pIndex = &_indexData[index])
            {
                ulong* pEndAddr = (ulong*) pIndex;
                pEndAddr++;
                *pEndAddr = endAddr;
            }
        }

        public unsafe void SetHintForKey(byte[] key, KeyHint hint)
        {
            var index = GetKeyIndexFromKey(key);
#if DEBUG
            // if (index > _indexData.Length-1)
            // {
            //     Console.WriteLine("DebugMe");   
            // }
#endif
            
            fixed (byte* pIndex = &_indexData[index])
            {
                (*(KeyHint*) pIndex) = hint;
            }
        }

        public int GetKeyIndexFromKey(HashBin key)
        {
            return GetKeyIndexFromKey(key.Hash);
        }
        
        public unsafe int GetKeyIndexFromKey(byte[] key)
        {
            if (key.Length < IndexKeyLen)
            {
                throw new ArgumentException("key must be larger than the key index");
            }

            int indexLocation = 0; 
            if (key.Length >= _ulongSize)
            {
                fixed (byte* pKey = &key[key.Length-_ulongSize])                
                {
                    //we could just use the first n bytes of key.
                    //but that makes it quite difficult to test.
                    //..and it's nice to have the data aligned e.g 0x01, 0x02
                    return (int)(((*(ulong*) pKey) >>_keyShift)<<4)+1;
                }
            }
            else
            {
                //they key isn't long enough for us to cast to ulong so loop through bytes
                for (int i = 0; i < IndexKeyLen; i++)
                {
                    var nextByte = key[i + key.Length - IndexKeyLen];
                    indexLocation += nextByte << (i<<3);
                }
                indexLocation <<= 4;//multiple by 16 (ulong is 8 bytes and there's 2 per index)
                return ++indexLocation;//offset by one for the header
            }
        }
        
        public unsafe ulong GetAddressForKey(byte[] key, bool getEndAddr = false)
        {
            //TODO: how can i also get the end addr key? would need to know where the next key starts :/
            var indexLocation = GetKeyIndexFromKey(key);
            if (indexLocation > 0)
            {
                fixed (byte* pIndex = &_indexData[indexLocation])
                {
                    if (getEndAddr)
                        return *(((ulong*) pIndex) + 1);
                    
                    return *(ulong*)pIndex;
                }
            }
            return 0;
        }

        public KeyHint GetAddressHintForKey(HashBin key)
        {
            return GetAddressHintForKey(key.Hash);
        }
        
        public unsafe KeyHint GetAddressHintForKey(byte[] key)
        {
            //TODO: how can i also get the end addr key? would need to know where the next key starts :/
            var indexLocation = GetKeyIndexFromKey(key);
            if (indexLocation > 0)
            {
                fixed (byte* pIndex = &_indexData[indexLocation])
                {
                    KeyHint* pHint = (KeyHint*) pIndex;
                    return *pHint;
                }
            }
            return new KeyHint();
        }


        

        // private void TypeCheck()
        // {
        //     if (!_validTypes.Contains(typeof(T)))
        //     {
        //         throw new ArgumentException("Type of T must one of: " + string.Concat(_validTypes, ", "));
        //     }
        // }
    }
    
    [StructLayout(LayoutKind.Sequential)]
    public struct KeyHint
    {
        public ulong StartAddr;
        public ulong EndAddr;
    }
}