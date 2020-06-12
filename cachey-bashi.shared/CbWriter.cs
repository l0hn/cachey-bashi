using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;

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
            List<string> batchFiles = new List<string>();
            var batchNameFormat = Path.Combine(cb.Dir, cb.DbName) + ".keybatch_{0}";
            var batchIndex = 0;
            
            //take batches of 100k? arbitraty or maybe roughly calc mem requirements
            var keyDataArray = new KeyData[100000];
            var index = 0;
            var datFileIndex = 0;
            var keyCount = (ulong)0;

            foreach (var kvp in data)
            {
                if (kvp.Key.Length != keyLength)
                    throw new ArgumentException($"All keys must be of the provided keyLength: {keyLength}");
                
                //need to copy the key array here incase someone is re-using the buffer
                keyDataArray[index].Key = kvp.Key.Clone();
                keyDataArray[index].DataAddr.addr = (ulong)datFileIndex;
                keyDataArray[index].DataAddr.len = (ulong)kvp.Value.Length;
                //todo: need to write the dat file here so we can discard data from memory
                cb.CbData.UnsafeWrite(kvp.Value);
                datFileIndex += kvp.Value.Length;

                var newBatch = index == keyDataArray.Length - 1;
                
                if (newBatch)//time to sort and start a new batch
                {
                    var batchFile = string.Format(batchNameFormat, batchIndex);
                    WriteBatch(keyDataArray, batchFile, keyDataArray.Length);
                    batchFiles.Add(batchFile);
                    batchIndex++;
                    index = 0;
                }

                if (!newBatch)
                    index++;
                
                keyCount++;
            }

            if (index > 0) //write the remaining keys to the final batch
            {
                var batchFile = string.Format(batchNameFormat, batchIndex);
                WriteBatch(keyDataArray, batchFile, (int)index);
                batchFiles.Add(batchFile);
            }

            //no more sorting required if only 1 batch so just write the file directly
            if (batchIndex == 1)
            {
                var outFile = cb.CbKey.FileStream;
                var writer = new BinaryWriter(outFile);
                writer.Write(keyCount);
                //first the keys
                foreach (var keyData in keyDataArray)
                {
                    outFile.Write(keyData.Key.Hash, 0, keyData.Key.Length);
                }
                //then the addr infos
                foreach (var keyData in keyDataArray)
                {
                    writer.Write(keyData.DataAddr.addr);
                    writer.Write(keyData.DataAddr.len);
                }
                return;
            }
            
            //now sort the batches into the final file
            SortAndWrite(batchFiles, keyCount, cb, keyLength, cb.KeyFile);
        }

        static void WriteBatch(KeyData[] keyDataArray, string outFile, int count)
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

        static void SortAndWrite(List<string> batchFiles, ulong keyCount, CacheyBashi cb, ushort keyLength, string outFile)
        {
            using var streams = new StreamCollection();
            var batches = new List<CurrentBatchInfo>();
            foreach (var batchFile in batchFiles)
            {
                var stream = File.OpenRead(batchFile);
                streams.Streams.Add(stream);
                batches.Add(new CurrentBatchInfo(keyLength, stream));
            }

            var outStream = cb.CbKey.FileStream;
            var writer = new BinaryWriter(outStream);
            writer.Write(keyCount);
            
            var remainingBatches = new List<CurrentBatchInfo>();
            remainingBatches.AddRange(batches);

            var currentKeyIndex = -1;
            var currentKeyRangeStartAddr = outStream.Position;

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
                    var end = outStream.Position - keyLength;
                    cb.CbIndex.SetHintForKey(lastHash.Hash, new KeyHint()
                    {
                        StartAddr = (ulong)currentKeyRangeStartAddr,
                        EndAddr = (ulong)end
                    });
                    currentKeyRangeStartAddr = outStream.Position;
                    currentKeyIndex = keyIndex;
                }
                
                //write the key
                outStream.Write(lowestBatch.CurrentHashBin.Hash, 0, lowestBatch.CurrentHashBin.Length);
                //we can't (be bothered) to attempt write this file sequentially so seek around a bit
                //SSDs are getting faster after all :)
                var pos = outStream.Position;
                var seekTo = (long) (addrOffset + (keysWritten * 16));
                outStream.Position = seekTo;
                writer.Write(lowestBatch.CurrentAddr.addr);
                writer.Write(lowestBatch.CurrentAddr.len);
                outStream.Position = pos;
                
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
                EndAddr = (ulong)outStream.Position-keyLength
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
        public FileStream Stream;
        public BinaryReader Reader;
        public DataAddr CurrentAddr;
        public HashBin CurrentHashBin;
        public bool Complete;
        private ushort _keyLength;

        public CurrentBatchInfo(ushort keyLength, FileStream stream)
        {
            _keyLength = keyLength;
            Stream = stream;
            Reader = new BinaryReader(Stream);
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
            CurrentAddr.addr = Reader.ReadUInt64();
            CurrentAddr.len = Reader.ReadUInt64();
            return true;
        }

        bool CheckComplete()
        {
            if (Stream.Position >= Stream.Length)
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
}