using System;
using System.IO;
using System.Threading.Tasks;

namespace cachey_bashi
{
    public class CbData: IDisposable
    {
        object LOCK = new object();
        private string _filePath;
        private FileStream _fileStream;
        
        public CbData(string datFile, bool createNew)
        {
            _filePath = datFile;
            if (createNew)
            {
                _fileStream = new FileStream(_filePath, FileMode.Create);
                return;
            }
            
            _fileStream = new FileStream(_filePath, FileMode.OpenOrCreate);
        }

        public byte[] GetValue(DataAddr addr)
        {
            var buf = new byte[addr.len];
            lock (LOCK)
            {
                _fileStream.Position = (long)addr.addr;
                _fileStream.Read(buf, 0, (int)addr.len);
            }
            return buf;
        }

        public void Write(byte[] value)
        {
            lock (LOCK)
            {
                _fileStream.Write(value, 0, value.Length);
            }
        }

        public void UnsafeWrite(byte[] value)
        {
            _fileStream.Write(value, 0, value.Length);
        }

        public async Task UnsafeWriteAsync(byte[] value)
        {
            await _fileStream.WriteAsync(value, 0, value.Length);
        }

        public void Dispose()
        {
            _fileStream?.Dispose();
        }
    }
}