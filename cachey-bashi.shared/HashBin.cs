using System;
using System.Collections;
using System.IO;
using System.Linq;
using System.Security.Policy;

namespace cachey_bashi
{
    public class HashBin: IComparable, IComparable<HashBin>
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

        
        
        public HashBin(Stream stream, int count)
        {
            
        }

        
        static unsafe int Compare(HashBin a, HashBin b)
        {
            if (a is null & !(b is null))
                return -1;

            if (b is null & !(a is null))
            {
                return 1;
            }
            
            if (a is null && b is null)
            {
                return 0;
            }

            var aLen = a._hash.Length;

            if (aLen < b._hash.Length)
                return -1;
            
            if (aLen > b._hash.Length)
                return 1;
            
            fixed (byte* pA = &a._hash[0])
            fixed (byte* pB = &b._hash[0])
            {
                ulong* pCurrentA = (ulong*)pA;
                ulong* pCurrentB = (ulong*)pB;
                for (int i = 0; i < aLen; i+=sizeof(ulong))
                {
                    if (*pCurrentA < *pCurrentB)
                        return -1;
                    if (*pCurrentA > *pCurrentB)
                        return 1;

                    pCurrentA++;
                    pCurrentB++;
                }
            }

            return 0;
        }
        
        public static unsafe bool operator ==(HashBin a, HashBin b)
        {
            return Compare(a, b) == 0;
        }

        public static bool operator !=(HashBin a, HashBin b)
        {
            return !(a == b);
        }

        public static bool operator >(HashBin a, HashBin b)
        {
            return a.CompareTo(b) == 1;
        }
        
        public static bool operator <(HashBin a, HashBin b)
        {
            return a.CompareTo(b) == -1;
        }

        public int CompareTo(HashBin other)
        {
            return Compare(this, other);
        }

        public override bool Equals(object obj)
        {
            return this == obj as HashBin;
        }

        public override string ToString()
        {
            return string.Concat(_hash.Take(_length).Select(i => i.ToString("x2")));
        }

        public int CompareTo(object obj)
        {
            var hB = obj as HashBin;
            if (hB == null)
                return 1;
            
            return CompareTo(hB);
        }
    }
}