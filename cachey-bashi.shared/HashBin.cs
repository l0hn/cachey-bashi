using System;
using System.IO;
using System.Linq;

namespace cachey_bashi
{
    public class HashBin
    {
        private byte[] _hash;
        private int _length;

        public int Length => _length;
        public byte[] Hash => _hash;

        public HashBin(string hexStr)
        {
            _hash = hexStr.HexToBytes();
            _length = _hash.Length;
        }
        
        public HashBin(byte[] hash)
        {
            _length = hash.Length;
            if (_length < sizeof(ulong))
            {
                _hash = new byte[sizeof(ulong)];
            }
            else if (_length % sizeof(ulong) != 0)
            {
                //padding required.
                _hash = new byte[_length+sizeof(ulong)];
            }
            else
            {
                _hash = new byte[_length];    
            }
            
            Array.Copy(hash, _hash, _length);
        }

        public HashBin(Stream stream, int length)
        {
            _length = length;
            if (length <= sizeof(ulong))
                _hash = new byte[sizeof(ulong)];
            else if (length % sizeof(ulong) == 0)
                _hash = new byte[length];
            else
                _hash = new byte[length+sizeof(ulong)];
                
            stream.Read(_hash, 0, length);
        }

        public static unsafe bool operator ==(HashBin a, HashBin b)
        {
            if ((a is null & !(b is null)) || (b is null & !(a is null)))
                return false;

            if (a is null && b is null)
            {
                return true;
            }

            var aLen = a._hash.Length;
            
            if (aLen != b._hash.Length)
            {
                return false;
            }

            fixed (byte* pA = &a._hash[0])
            fixed (byte* pB = &b._hash[0])
            {
                ulong* pCurrentA = (ulong*)pA;
                ulong* pCurrentB = (ulong*)pB;
                for (int i = 0; i < aLen; i+=sizeof(ulong))
                {
                    if (*pCurrentA != *pCurrentB)
                    {
                        return false;
                    }

                    pCurrentA++;
                    pCurrentB++;
                }
            }

            return true;
        }

        public static bool operator !=(HashBin a, HashBin b)
        {
            return !(a == b);
        }

        public override bool Equals(object? obj)
        {
            return this == obj as HashBin;
        }

        public override string ToString()
        {
            return string.Concat(_hash.Take(_length).Select(i => i.ToString("x2")));
        }
    }
}