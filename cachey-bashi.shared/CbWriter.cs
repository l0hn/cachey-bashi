using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using HPCsharp;

namespace cachey_bashi
{
    /// <summary>
    /// Assumes equal length keys
    /// </summary>
    public static class CbWriter
    {
        public static void Write(CacheyBashi cb, ushort keyLength, IEnumerable<KeyValuePair<byte[], byte[]>> data)
        {
            Write(cb, keyLength, data.Select(i => new KeyValuePair<HashBin, byte[]>(i.Key.ToHashBin(), i.Value)));
        }
        
        public static void Write(CacheyBashi cb, ushort keyLength, IEnumerable<KeyValuePair<HashBin, byte[]>> data)
        {
            var sw = new Stopwatch();
            sw.Start();
            List<string> batchFiles = new List<string>();
            var batchNameFormat = Path.Combine(cb.Dir, cb.DbName) + ".keybatch_{0}";
            var batchIndex = 0;
            
            //take batches of 100k? arbitraty or maybe roughly calc mem requirements
            //use 2 buffers, one for writing, and one for streaming out to file?
            var keyDataHolders = new KeyDataHolder[8];
            var keyDataArray1 = new KeyData[100000];
            var keyDataArray2 = new KeyData[100000];
            var activeKeyDataArray = keyDataArray1;
            var index = 0;
            var datFileIndex = 0;
            var keyCount = (ulong)0;
            Task batchWriteTask = null;

            foreach (var kvp in data)
            {
                if (kvp.Key.Length != keyLength)
                    throw new ArgumentException($"All keys must be of the provided keyLength: {keyLength}");
                
                //need to copy the key array here incase someone is re-using the buffer
                activeKeyDataArray[index].Key = kvp.Key.Clone();
                activeKeyDataArray[index].DataAddr.addr = (ulong)datFileIndex;
                activeKeyDataArray[index].DataAddr.len = (ulong)kvp.Value.Length;
                //need to write the dat file here so we can discard data from memory
                //cleanup tasks
                cb.CbData.UnsafeWrite(kvp.Value);
                datFileIndex += kvp.Value.Length;

                var newBatch = index == activeKeyDataArray.Length - 1;
                
                if (newBatch)//time to sort and start a new batch
                {
                    var batchFile = string.Format(batchNameFormat, batchIndex);

                    batchWriteTask?.Wait();
                    batchWriteTask?.Dispose();
                    var array = activeKeyDataArray;
                    batchWriteTask = Task.Run(() =>
                    {
                        WriteBatch(array, batchFile, array.Length);
                    });
                        
                    batchFiles.Add(batchFile);
                    batchIndex++;
                    index = 0;
                    //swap the active buffer
                    if (activeKeyDataArray == keyDataArray1)
                    {
                        activeKeyDataArray = keyDataArray2;
                    }
                    else
                    {
                        activeKeyDataArray = keyDataArray1;
                    }
                }

                if (!newBatch)
                    index++;
                
                keyCount++;
            }

            //did we finish processing exactly on a batch boundary?
            //if so roll back a batch index.
            if (index == 0 && batchIndex > 0)
            {
                batchIndex--;
            }

            if (batchWriteTask != null && !batchWriteTask.IsCompleted)
            {
                batchWriteTask.Wait();
            }

            if (index > 0) //write the remaining keys to the final batch
            {
                var batchFile = string.Format(batchNameFormat, batchIndex);
                WriteBatch(activeKeyDataArray, batchFile, (int)index);
                batchFiles.Add(batchFile);
            }

            Console.WriteLine($"writing batches took: {sw.ElapsedMilliseconds}");

            //no more sorting required if only 1 batch so just write the file directly
            if (batchIndex == 1)
            {
                var outFile = cb.CbKey.FileStream;
                var writer = new BinaryWriter(outFile);
                writer.Write(keyCount);
                //first the keys
                foreach (var keyData in activeKeyDataArray)
                {
                    outFile.Write(keyData.Key.Hash, 0, keyData.Key.Length);
                }
                //then the addr infos
                foreach (var keyData in activeKeyDataArray)
                {
                    writer.Write(keyData.DataAddr.addr);
                    writer.Write(keyData.DataAddr.len);
                }
                return;
            }
            
            sw.Restart();
            //now sort the batches into the final file
            SortAndWrite(batchFiles, keyCount, cb, keyLength, cb.KeyFile);
            Console.WriteLine($"sorting batches and writing took: {sw.ElapsedMilliseconds}");
        }

        static void WriteBatch(KeyData[] keyDataArray, string outFile, int count)
        {
             WriteBatchArraySort(keyDataArray, outFile, count);
            //WriteBatchLinqSort(keyDataArray, outFile, count);
            //WriteBatchHpcSort(keyDataArray, outFile, count);
        }
        
        static void WriteBatchArraySort(KeyData[] keyDataArray, string outFile, int count)
        {
            Array.Sort(keyDataArray, 0, count, new KeyDataComparer());
            //write to batch file
            
            using var stream = new FileStream(outFile, FileMode.Create);
            var writer = new BinaryWriter(stream);
            for (int i = 0; i < count; i++)
            {
                stream.Write(keyDataArray[i].Key.Hash, 0, keyDataArray[i].Key.Length);
                writer.Write(keyDataArray[i].DataAddr.addr);
                writer.Write(keyDataArray[i].DataAddr.len);
            }
            
        } 
        
        static void WriteBatchLinqSort(KeyData[] keyDataArray, string outFile, int count)
        {
            //Array.Sort(keyDataArray, 0, count, new KeyDataComparer());
            //write to batch file
            
            using var stream = new FileStream(outFile, FileMode.Create);
            var writer = new BinaryWriter(stream);
            foreach (var keyData in keyDataArray.Take(count).AsParallel().OrderBy(i => i.Key))
            {
                stream.Write(keyData.Key.Hash, 0, keyData.Key.Length);
                writer.Write(keyData.DataAddr.addr);
                writer.Write(keyData.DataAddr.len);
            }
            
        } 
        
        static void WriteBatchHpcSort(KeyData[] keyDataArray, string outFile, int count)
        {
            var data = keyDataArray.SortMergePar(0, count, new KeyDataComparer());
            //write to batch file
            
            using var stream = new FileStream(outFile, FileMode.Create);
            var writer = new BinaryWriter(stream);
            for (int i = 0; i < count; i++)
            {
                stream.Write(data[i].Key.Hash, 0, data[i].Key.Length);
                writer.Write(data[i].DataAddr.addr);
                writer.Write(data[i].DataAddr.len);
            }
        } 

        static void SortAndWrite(List<string> batchFiles, ulong keyCount, CacheyBashi cb, ushort keyLength, string outFile)
        {
            using var streams = new StreamCollection();
            var batches = new List<CurrentBatchInfo>();
            foreach (var batchFile in batchFiles)
            {
                var stream = new FileStream(batchFile, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);//File.OpenRead(batchFile);
                streams.Streams.Add(stream);
                batches.Add(new CurrentBatchInfo(keyLength, stream));
            }

            var cbKeyFileStream = cb.CbKey.FileStream;
            var cbKeyWritter = new BinaryWriter(cbKeyFileStream);
            cbKeyWritter.Write(keyCount);

            var cbKeyAddrFileStream = cb.CbKey.AddrsFileStream;
            var cbKeyAddrsWritter = new BinaryWriter(cbKeyAddrFileStream);
            
            var remainingBatches = new List<CurrentBatchInfo>();
            remainingBatches.AddRange(batches);

            var currentKeyIndex = -1;
            var currentKeyRangeStartAddr = cbKeyFileStream.Position;

            ulong keysWritten = 0;
            var addrOffset = cb.CbKey.HeaderLength + (keyCount * keyLength);
            HashBin lastHash = null;

// #if DEBUG
//             var debugHash = new HashBin("0000000000000000000000000000d8f4");
// #endif
            
            while (remainingBatches.Count > 0)
            {
                var lowestBatch = remainingBatches.OrderBy(i => i.CurrentHashBin).First();

// #if DEBUG
//                 if (lowestBatch.CurrentHashBin == null)
//                 {
//                     Console.WriteLine("DebugMe");
//                 }
//                 
//                 if (debugHash == lowestBatch.CurrentHashBin)
//                 {
//                     Console.WriteLine("DebugMe");
//                 }
// #endif

                //update the index if we've reached the end of a key range
                var keyIndex = cb.CbIndex.GetKeyIndexFromKey(lowestBatch.CurrentHashBin);
                if (currentKeyIndex == -1)
                {
                    currentKeyIndex = keyIndex;
                }
                else if(currentKeyIndex != keyIndex)//we've reached the end of a key range
                {
                    var end = cbKeyFileStream.Position - keyLength;
                    cb.CbIndex.SetHintForKey(lastHash.Hash, new KeyHint()
                    {
                        StartAddr = (ulong)currentKeyRangeStartAddr,
                        EndAddr = (ulong)end
                    });
                    currentKeyRangeStartAddr = cbKeyFileStream.Position;
                    currentKeyIndex = keyIndex;
                }
                
                //write the key to the key  file
                cbKeyFileStream.Write(lowestBatch.CurrentHashBin.Hash, 0, lowestBatch.CurrentHashBin.Length);
                
                //write the data addrs to the addr file
                cbKeyAddrsWritter.Write(lowestBatch.CurrentAddr.addr);
                cbKeyAddrsWritter.Write(lowestBatch.CurrentAddr.len);

                //finally move to the next key in the batch
                lastHash = lowestBatch.CurrentHashBin;

                if (!lowestBatch.Next())
                    remainingBatches.Remove(lowestBatch);
                
                keysWritten++;
            }
            
            //don't forget to set the last item's key hint!
            cb.CbIndex.SetHintForKey(lastHash.Hash, new KeyHint()
            {
                StartAddr = (ulong)currentKeyRangeStartAddr,
                EndAddr = (ulong)cbKeyFileStream.Position-keyLength
            });
            
            //write the index out to disk
            cb.CbIndex.WriteToDisk();
            
            //tell cbKey to update stats
            cb.CbKey.PostWriteUpdate();
            
            //cleanup the batch files.
            streams.Dispose();
            foreach (var batchFile in batchFiles)
            {
                File.Delete(batchFile);
            }
        }
    }
   
    [StructLayout(LayoutKind.Sequential)]
    struct KeyData
    {
        public DataAddr DataAddr;
        public HashBin Key;
    }

    class KeyDataComparer: IComparer<KeyData>
    {
        public int Compare(KeyData x, KeyData y)
        {
            return x.Key.CompareTo(y.Key);
        }
    }

    class CurrentBatchInfo
    {
        public Stream Stream;
        public BinaryReader Reader;
        public DataAddr CurrentAddr;
        public HashBin CurrentHashBin;
        public bool Complete;
        private ushort _keyLength;

        private long _streamLength;
        private long _bytesRead;
        
        public CurrentBatchInfo(ushort keyLength, Stream stream)
        {
            _keyLength = keyLength;
            Stream = stream;
            Reader = new BinaryReader(stream);
            _streamLength = stream.Length;
            _bytesRead = stream.Position;
            Next();
        }
        
        public bool Next()
        {
            if (CheckComplete())
            {
                CurrentHashBin = null;
                return false;
            }
            
            CurrentHashBin = new HashBin(Stream, _keyLength);
            _bytesRead += _keyLength;
            CurrentAddr.addr = Reader.ReadUInt64();
            CurrentAddr.len = Reader.ReadUInt64();
            _bytesRead += 16;
            return true;
        }

        bool CheckComplete()
        {
            if (_bytesRead >= _streamLength)
            {
                Complete = true;
                return true;
            }

            return false;
        }
    }

    class StreamCollection: IDisposable
    {
        public List<Stream> Streams { get; set; } = new List<Stream>();

        public void Dispose()
        {
            foreach (var stream in Streams)
            {
                stream?.Dispose();
            }
        }
    }

    class KeyDataHolder: IDisposable
    {
        public KeyData[] KeyDataArray { get; private set; }
        public ManualResetEvent Sync { get; private set; }

        public KeyDataHolder()
        {
            KeyDataArray = new KeyData[100000];
            Sync = new ManualResetEvent(true);
        }
        
        public static KeyDataHolder[] CreateMany(int count)
        {
            var result = new KeyDataHolder[count];
            for (int i = 0; i < count; i++)
            {
                result[0] = new KeyDataHolder();
            }

            return result;
        }

        public void Dispose()
        {
            Sync?.Dispose();
        }
    }
}