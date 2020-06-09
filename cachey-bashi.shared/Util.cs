using System;
using System.Linq;
using System.Security.Cryptography;

namespace cachey_bashi
{
    public static class Util
    {
        public static string ToHexString(this byte[] bytes)
        {
            return string.Concat(bytes.Select(i => i.ToString("x2")));
        }

        public static byte[] HexToBytes(this string hexString)
        {
            if (hexString.Length % 2 == 1)
            {
                throw new ArgumentException("Invalid hex string. Must be even number length");
            }

            byte[] buf = new byte[hexString.Length >> 1];

            for (int i = 0; i < hexString.Length-1; i+=2)
            {
                buf[i >> 1] = Convert.ToByte(hexString.Substring(i, 2), 16);
            }

            return buf;
        }

        public static byte[] Md5(this byte[] data)
        {
            using MD5 m = MD5.Create();
            return m.ComputeHash(data);
        }

        public static byte[] Sha256(this byte[] data)
        {
            using SHA256 sha = SHA256.Create();
            return sha.ComputeHash(data);
        }
    }
}