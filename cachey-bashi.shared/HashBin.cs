using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;

namespace cachey_bashi
{
    //todo: when using pointers or partial arrays, Hash property is invalid, need to fix.
    public unsafe class HashBin: IComparable, IComparable<HashBin>
    {
        private byte[] _hash;
        private int _length;

        public int Length => _length;

        public byte[] Hash => _hash;

        private int _partialStart;

        public byte* PStart { get; private set; }
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

        public void SetPointer(byte* pStart, int count)
        {
            PStart = pStart;
            _length = count;
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

        static int ArrayPtrCompare(byte* arrA, byte* arrB, int count)
        {
            // byte* nextA = arrA + count - (count > 7 ? 8 : 1);
            // byte* nextB = arrB + count - (count > 7 ? 8 : 1);
            while (count > 0)
            {
                if (count>7)
                {
                    count -= 8;
                    if (*(ulong*)(arrA+count) == *(ulong*)(arrB+count))
                    {
                        continue;
                    }

                    if (*(ulong*)(arrA+count) > *(ulong*)(arrB+count))
                        return 1;

                    return -1;
                }

                count--;
                if (arrA[count] == arrB[count])
                {
                    continue;
                }

                if (arrA[count] > arrB[count])
                    return 1;

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
                if (a.PStart != (byte*)0 && b.PStart != (byte*)0)
                {
                    return ArrayPtrCompare(a.PStart, b.PStart, a._length);
                }
                if (a.PStart == (byte*)0 && b.PStart != (byte*)0)
                {
                    fixed (byte* aStart = &a._hash[a._partialStart])
                    {
                        return ArrayPtrCompare(aStart, b.PStart, a._length);    
                    }
                }
                if (a.PStart != (byte*)0 && b.PStart == (byte*)0)
                {
                    fixed (byte* bStart = &b._hash[b._partialStart])
                    {
                        return ArrayPtrCompare(a.PStart, bStart, a._length);    
                    }
                }
                
                fixed (
                    byte* pA = &a._hash[a._partialStart],
                    pB = &b._hash[b._partialStart])
                {
                    return ArrayPtrCompare(pA, pB, a._length);
                }
            }

            if (a._length < b._length)
                return -1;
            
            if (a._length > b._length)
                return 1;

            return 0;
        }
        
        public static unsafe bool operator ==(HashBin a, HashBin b)
        {
            return UnsafeAreEqual(a, b, 0);
        }

        private static byte* _nullPtr = (byte*) 0;
        
        public static bool UnsafeAreEqual(HashBin a, HashBin b, int count)
        {
            if (a is null && b is null)
                return true;

            if (a is null || b is null)
                return false;

            if (a._length != b._length)
            {
                return false;
            }

            count = count > 0 ? count : a._length;
            
            if (a.PStart != _nullPtr && b.PStart != _nullPtr)
            {
                return ArrayPtrEqualCompare(a.PStart, b.PStart, count);
            }

            if (a.PStart == _nullPtr && b.PStart == _nullPtr)
            {
                fixed (byte* aStart = &a._hash[0], bStart = &b._hash[0])
                {
                    return ArrayPtrEqualCompare(aStart, bStart, count);
                }
            }
            
            if (a.PStart == _nullPtr)
            {
                fixed (byte* aStart = &a._hash[0])
                {
                    return ArrayPtrEqualCompare(aStart, b.PStart, count);
                }
            }
            
            fixed (byte* bStart = &a._hash[0])
            {
                return ArrayPtrEqualCompare(a.PStart, bStart, count);
            }
        }

        public static bool ArrayPtrEqualCompare(byte* a, byte* b, int count)
        {
            for (int i = 0; i < count;)
            {
                if (count-i > 7)
                {
                    if (*(long*)(a+i) != *(long*)(b+i))
                    {
                        return false;
                    }

                    i += 8;
                    continue;
                }
                if (a[i] != b[i])
                {
                    return false;
                }

                i++;
            }
            return true;
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


        public HashBin Clone(bool shallow = false)
        {
            return new HashBin(_hash, !shallow);
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