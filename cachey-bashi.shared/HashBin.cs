using System;
using System.Diagnostics;
using System.IO;
using System.Linq;

namespace cachey_bashi
{
    public class HashBin: IComparable, IComparable<HashBin>
    {
        private byte[] _hash;
        private int _length;

        public int Length => _length;

        public byte[] Hash => _hash;

        private int _partialStart;
        private int _partialEnd;
        
        
        public HashBin(string hexStr)
        {
            _hash = hexStr.HexToBytes();
            _length = _hash.Length;
            _partialEnd = _length - 1;
        }
        
        public HashBin(byte[] hash, bool copy = true)
        {
            _length = hash.Length;
            _partialEnd = _length - 1;
            if (copy)
            {
                _hash = new byte[hash.Length];
                Array.Copy(hash, _hash, hash.Length);
            }
            else
            {
                _hash = hash;
            }
        }

        public HashBin(Stream stream, int count)
        {
            _length = count;
            _partialEnd = _length - 1;
            _hash = new byte[_length];
#if DEBUG
            var sw = new Stopwatch();
#endif
            var read = stream.Read(_hash, 0, count);
#if DEBUG
            if (sw.ElapsedMilliseconds > 4)
            {
                Console.WriteLine("Slow file read");
            }
#endif
            if (_length != read)
            {
                throw new ArgumentException($"Could not read {count} bytes from the provided stream (got: {read})");
            }
        }

        /// <summary>
        /// Dragons are here. Don't use unless necessary for speed
        /// </summary>
        /// <param name="array"></param>
        /// <param name="start"></param>
        /// <param name="count"></param>
        /// <param name="copy"></param>
        public void SetFromPartialArray(byte[] array, int start, int count, bool copy = true)
        {
            if (copy)
            {
                Array.Copy(array, start, _hash, 0, count);
                _length = count;
                _partialEnd = count - 1;
                return;
            }

            _hash = array;
            _length = count;
            _partialEnd = start + count - 1;
            _partialStart = start;
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

            var aLen = a._length;

            if (aLen < b._length)
                return -1;
            
            if (aLen > b._length)
                return 1;
            
            fixed (byte* pA = &a._hash[a._partialEnd])
            fixed (byte* pB = &b._hash[b._partialEnd])
            fixed (byte* pAEnd = &a._hash[a._partialStart])
            {
                ulong* pCurrentA = (ulong*) (pA+1);
                ulong* pCurrentB = (ulong*) (pB+1);
                
                while (pCurrentA-(ulong*)pAEnd > 0)
                {
                    pCurrentA -= 1;
                    pCurrentB -= 1;
                    
                    if (*pCurrentA < *pCurrentB)
                        return -1;
                    if (*pCurrentA > *pCurrentB)
                        return 1;
                }
                
                if (pCurrentA == pAEnd) //this means the array was divisible by sizeof(ulong)
                    return 0;
                
                //now check the remaining byte one at a time
                //todo: could probably write a utility that tapers down from ulong > uint > ushort > byte checks but meh
                byte* pCurrentAByte = (byte*) pCurrentA;
                byte* pCurrentBByte = (byte*) pCurrentB;
                
                while (pCurrentAByte-pAEnd > 0)
                {
                    pCurrentAByte--;
                    pCurrentBByte--;
                    
                    if (*pCurrentAByte < *pCurrentBByte)
                        return -1;
                    if (*pCurrentAByte > *pCurrentBByte)
                        return 1;
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

    public static class HashBinExtensions
    {
        public static HashBin ToHashBin(this byte[] buf, bool copy = true)
        {
            return new HashBin(buf, copy);
        }
    }
}