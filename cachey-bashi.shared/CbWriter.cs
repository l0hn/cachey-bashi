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
            List<string> batchFiles = new List<string>();
            var batchNameFormat = Path.Combine(cb.Dir, cb.DbName) + ".keybatch_{0}";
            var batchIndex = 0;
            
            //take batches of 100k? arbitraty or maybe roughly calc mem requirements
            var keyDataArray = new KeyData[100000];
            var index = (ulong)0;
            var datFileIndex = 0;

            foreach (var kvp in data)
            {
                if (kvp.Key.Length != keyLength)
                    throw new ArgumentException($"All keys must be of the provided keyLength: {keyLength}");
                
                keyDataArray[index].Key=kvp.Key;
                keyDataArray[index].DataAddr.addr = (ulong)datFileIndex;
                keyDataArray[index].DataAddr.len = (ulong)kvp.Value.Length;
                //todo: need to write the dat file here so we can discard data from memory
                //datFile.Write(kvp.Value etc..);
                datFileIndex += kvp.Value.Length;

                if (index == (ulong)keyDataArray.Length-1)//time to sort and start a new batch
                {
                    Array.Sort(keyDataArray, (keyData, keyData2) =>
                    {
                        var hA = new HashBin(keyData.Key);
                        var hB = new HashBin(keyData2.Key);
                        return hA.CompareTo(hB);
                    });
                    //write to batch file
                    var batchFile = string.Format(batchNameFormat, batchIndex);
                    using var stream = new FileStream(batchFile, FileMode.CreateNew);
                    var writer = new BinaryWriter(stream);
                    foreach (var keyData in keyDataArray)
                    {
                        stream.Write(keyData.Key, 0, keyData.Key.Length);
                        writer.Write(keyData.DataAddr.addr);
                        writer.Write(keyData.DataAddr.len);
                    }
                    batchFiles.Add(batchFile);
                    batchIndex++;
                }

                index++;
            }

            var keyCount = index;
            
            //no more sorting required if only 1 batch so just write the file directly
            if (batchIndex == 1)
            {
                var outFile = cb.CbKey.FileStream;
                var writer = new BinaryWriter(outFile);
                writer.Write(keyCount);
                //first the keys
                foreach (var keyData in keyDataArray)
                {
                    outFile.Write(keyData.Key, 0, keyData.Key.Length);
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
            
            while (remainingBatches.Count > 0)
            {
                var lowestBatch = remainingBatches.OrderBy(i => i.CurrentHashBin).First();
                
                if (lowestBatch.Complete)
                    remainingBatches.Remove(lowestBatch);

                //update the index if we've reached the end of a key range
                var keyIndex = cb.CbIndex.GetKeyIndexFromKey(lowestBatch.CurrentHashBin);
                if (currentKeyIndex == -1)
                {
                    currentKeyIndex = keyIndex;
                }
                else if(currentKeyIndex != keyIndex)//we've reached the end of a key range
                {
                    cb.CbIndex.SetHintForKey(lowestBatch.CurrentHashBin.Hash, new KeyHint()
                    {
                        StartAddr = (ulong)currentKeyRangeStartAddr,
                        EndAddr = (ulong)outStream.Position
                    });
                    currentKeyRangeStartAddr = outStream.Position + keyLength;
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
                lowestBatch.Next();
                keysWritten++;
            }
            
            //write the index out to disk
            cb.CbIndex.WriteToDisk();
            
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
        public byte[] Key;
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
        
        public void Next()
        {
            if (CheckComplete())
                return;
            
            CurrentHashBin = new HashBin(Stream, _keyLength);
            CurrentAddr.addr = Reader.ReadUInt64();
            CurrentAddr.len = Reader.ReadUInt64();
            CheckComplete();
        }

        bool CheckComplete()
        {
            if (Stream.Position == Stream.Length-1)
            {
                Complete = true;
                return true;
            }

            return false;
        }
    }

    class StreamCollection: IDisposable
    {
        public List<Stream> Streams { get; set; }

        public void Dispose()
        {
            foreach (var stream in Streams)
            {
                stream?.Dispose();
            }
        }
    }
}