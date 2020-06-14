using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;

namespace cachey_bashi
{
    public class HashBin: IComparable, IComparable<HashBin>
    {
        private byte[] _hash;
        private int _length;

        public int Length => _length;

        public byte[] Hash => _hash;

        private int _partialStart;
        // private int _partialEnd;
        
        
        public HashBin(string hexStr)
        {
            _hash = hexStr.HexToBytes();
            _length = _hash.Length;
            // _partialEnd = _length - 1;
        }
        
        public HashBin(byte[] hash, bool copy = true)
        {
            _length = hash.Length;
            // _partialEnd = _length - 1;
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
            // _partialEnd = _length - 1;
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
                // _partialEnd = count - 1;
                return;
            }

            _hash = array;
            _length = count;
            // _partialEnd = start + count - 1;
            _partialStart = start;
        }

        public void SetPartialIndexes(int start)
        {
            _partialStart = start;
            // _partialEnd = start + _length;
        }


        static int Compare(HashBin a, HashBin b)
        {
            return UnsafeCompare(a, b);
        }
        
        static int SafeCompare(HashBin a, HashBin b)
        {
            if (a is null)
                return b is null ? 0 : -1;

            if (b is null)
                return 1;
            
            var aLen = a._length;

            if (aLen < b._length)
                return -1;
            
            if (aLen > b._length)
                return 1;

            byte nextA;
            byte nextB;
            
            for (int i = aLen-1; i >= 0; i--)
            {
                nextA = a._hash[i + a._partialStart];
                nextB = b._hash[i + b._partialStart];
                if (nextA > nextB)
                    return 1;
                if (nextA < nextB)
                    return -1;
            }

            return 0;
        }

        static unsafe int UnsafeCompare(HashBin a, HashBin b)
        {
            if (a is null)
                return b is null ? 0 : -1;

            if (b is null)
                return 1;

            if (a._length == b._length)
            {
                // ulong* nextAlong;
                // ulong* nextBLong;
                byte* nextA;
                byte* nextB;
                int remain = a._length;
                int aOffset = a._partialStart + a._length - (remain > 7 ? 8 : 1);
                int bOffset = b._partialStart + b._length - (remain > 7 ? 8 : 1);
                
                fixed (byte* pA = &a._hash[aOffset], pB = &b._hash[bOffset])
                {
                    nextA = pA;
                    nextB = pB;

                    while (remain > 0)
                    {
                        if (remain>7)
                        {
                            if (*(ulong*)nextA == *(ulong*)nextB)
                            {
                                nextA -= 8;
                                nextB -= 8;
                                remain -= 8;
                                continue;
                            }

                            if (*(ulong*)nextA > *(ulong*)nextB)
                                return 1;

                            return -1;
                        }

                        if (*nextA == *nextB)
                        {
                            nextA--;
                            nextB--;
                            remain--;
                            continue;
                        }

                        if (*nextA > *nextB)
                            return 1;

                        return -1;
                    }
                }

                return 0;
            }

            if (a._length < b._length)
                return -1;
            
            if (a._length > b._length)
                return 1;

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


        public HashBin Clone()
        {
            return new HashBin(_hash, true);
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