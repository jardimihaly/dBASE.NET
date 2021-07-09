namespace Aronic.dBASE.NET
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Reflection;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;

    /// <summary>
    /// The Dbf class encapsulated a dBASE table (.dbf) file, allowing
    /// reading from disk, writing to disk, enumerating fields and enumerating records.
    /// </summary>
    public class Dbf
    {
        private DbfHeader header;

        /// <summary>
        /// Initializes a new instance of the <see cref="Dbf" />.
        /// </summary>
        public Dbf()
        {
            header = DbfHeader.CreateHeader(DbfVersion.FoxBaseDBase3NoMemo);
            Fields = new List<DbfField>();
            Records = new List<DbfRecord>();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Dbf" /> with custom encoding.
        /// </summary>
        /// <param name="encoding">Custom encoding.</param>
        public Dbf(Encoding encoding)
            : this()
        {
            Encoding = encoding ?? throw new ArgumentNullException(nameof(encoding));
        }

        /// <summary>
        /// The collection of <see cref="DbfField" /> that represent table header.
        /// </summary>
        public List<DbfField> Fields { get; }

        /// <summary>
        /// The collection of <see cref="DbfRecord" /> that contains table data.
        /// </summary>
        public List<DbfRecord> Records { get; }

        /// <summary>
        /// The <see cref="System.Text.Encoding" /> class that corresponds to the specified code page.
        /// Default value is <see cref="Encoding.ASCII" />
        /// </summary>
        public Encoding Encoding { get; } = Encoding.ASCII;

        /// <summary>
        /// Creates a new <see cref="DbfRecord" /> with the same schema as the table.
        /// </summary>
        /// <returns>A <see cref="DbfRecord" /> with the same schema as the <see cref="T:System.Data.DataTable" />.</returns>
        public DbfRecord CreateRecord()
        {
            DbfRecord record = new DbfRecord(Fields);
            Records.Add(record);
            return record;
        }

        public DbfRecord CreateRecord<T>(T entity)
            where T : IDbfBaseEntity
        {
            var record = CreateRecord();
            record.FromEntity(entity);
            return record;
        }

        /// <summary>
        /// Add a list of entities to the DBF.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="entities"></param>
        /// <returns></returns>
        public IEnumerable<DbfRecord> AddEntities<T>(IEnumerable<T> entities)
            where T : IDbfBaseEntity
        {
            var records = new List<DbfRecord>(Records.Count());
            if (entities.Count() > 0)
            {
                var properties = DbfRecord.GetDecoratedProperties(entities.First());

                foreach (var entity in entities)
                {
                    var record = CreateRecord();
                    record.FromEntity(entity, properties);
                    records.Add(record);
                }
            }

            return records;
        }

        /// <summary>
        /// Get records from the DBF mapped into entities.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public IEnumerable<T> GetEntities<T>()
            where T : IDbfBaseEntity
        {
            if (Records.Count() == 0)
            {
                return new T[0];
            }

            var entities = new List<T>(Records.Count());
            var templateObject = (T)Activator.CreateInstance(typeof(T));
            var properties = DbfRecord.GetDecoratedProperties(templateObject);
            foreach (var record in Records)
            {
                var entity = (T)Activator.CreateInstance(typeof(T));
                record.ToEntity(entity, properties);
                entities.Add(entity);
            }

            return entities;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public async Task<IEnumerable<T>> GetEntitiesAsync<T>()
            where T : IDbfBaseEntity
        {
            if(Records.Count == 0)
            {
                return new T[0];
            }

            var output = new T[Records.Count];

            var templateObject = (T)Activator.CreateInstance(typeof(T));
            var properties = DbfRecord.GetDecoratedProperties(templateObject);

            if (Environment.ProcessorCount < Records.Count)
            {
                var tasks = new List<Task>(Environment.ProcessorCount + 1);
                double chunksize = (double)Records.Count / Environment.ProcessorCount;

                if(chunksize == (int)chunksize)
                {
                    for (int i = 0; i < Records.Count; i += (int)chunksize)
                    {
                        int start = i;
                        int end = i + (int)chunksize;
                        tasks.Add(
                            Task.Run(() => ReadEntityRange(
                                output, 
                                start,
                                end, 
                                properties
                            ))
                        );
                    }
                }
                else
                {
                    int chunkLow = (int)chunksize;
                    for (int i = 0; i < Records.Count; i += chunkLow)
                    {
                        int start = i;
                        int end = i + chunkLow;
                        if(end > Records.Count)
                        {
                            end = Records.Count;
                        }
                        tasks.Add(
                            Task.Run(() => ReadEntityRange(output, start, end, properties))
                        );
                    }
                }
                await Task.WhenAll(tasks);
                return output;
            }
            else
            {
                return GetEntities<T>();
            }
        }

        private void ReadEntityRange<T>(T[] target, int from, int to, PropertyInfo[] properties)
            where T : IDbfBaseEntity
        {
            Console.WriteLine($"{from} -> {to}");
            for(int i = from; i < to; i++)
            {
                var entity = (T)Activator.CreateInstance(typeof(T));
                Records[i].ToEntity(entity, properties);
                target[i] = entity;
            }
        }

        /// <summary>
        /// Opens a DBF file, reads the contents that initialize the current instance, and then closes the file.
        /// </summary>
        /// <param name="path">The file to read.</param>
        public void Read(string path)
        {
            // Open stream for reading.
            using (FileStream baseStream = File.Open(path, FileMode.Open, FileAccess.Read))
            {
                string memoPath = GetMemoPath(path);
                if (memoPath == null)
                {
                    Read(baseStream);
                }
                else
                {
                    using (FileStream memoStream = File.Open(memoPath, FileMode.Open, FileAccess.Read))
                    {
                        Read(baseStream, memoStream);
                    }
                }
            }
        }

        /// <summary>
        /// Reads the contents of streams that initialize the current instance.
        /// </summary>
        /// <param name="baseStream">Stream with a database.</param>
        /// <param name="memoStream">Stream with a memo.</param>
        public void Read(Stream baseStream, Stream memoStream = null)
        {
            if (baseStream == null)
            {
                throw new ArgumentNullException(nameof(baseStream));
            }
            if (!baseStream.CanSeek)
            {
                throw new InvalidOperationException("The stream must provide positioning (support Seek method).");
            }

            baseStream.Seek(0, SeekOrigin.Begin);
            using (BinaryReader reader = new BinaryReader(baseStream))
            {
                ReadHeader(reader);
                byte[] memoData = memoStream != null ? ReadMemos(memoStream) : null;
                ReadFields(reader);

                // After reading the fields, we move the read pointer to the beginning
                // of the records, as indicated by the "HeaderLength" value in the header.
                baseStream.Seek(header.HeaderLength, SeekOrigin.Begin);
                ReadRecords(reader, memoData);
            }
        }

        /// <summary>
        /// Creates a new file, writes the current instance to the file, and then closes the file. If the target file already exists, it is overwritten.
        /// </summary>
        /// <param name="path">The file to read.</param>
        /// <param name="version">The version <see cref="DbfVersion" />. If unknown specified, use current header version.</param>
        public void Write(string path, DbfVersion version = DbfVersion.Unknown)
        {
            if (version != DbfVersion.Unknown)
            {
                header.Version = version;
                header = DbfHeader.CreateHeader(header.Version);
            }

            using (FileStream stream = File.Open(path, FileMode.Create, FileAccess.Write))
            {
                Write(stream, false);
            }
        }

        /// <summary>
        /// Creates writes the current instance to the specified stream.
        /// </summary>
        /// <param name="stream">The output stream.</param>
        /// <param name="version">The version <see cref="DbfVersion" />. If unknown specified, use current header version.</param>
        public void Write(Stream stream, DbfVersion version = DbfVersion.Unknown)
        {
            if (version != DbfVersion.Unknown)
            {
                header.Version = version;
                header = DbfHeader.CreateHeader(header.Version);
            }

            Write(stream, true);
        }

        private void Write(Stream stream, bool leaveOpen)
        {
            using (BinaryWriter writer = new BinaryWriter(stream, Encoding, leaveOpen))
            {
                header.Write(writer, Fields, Records);
                WriteFields(writer);
                WriteRecords(writer);
            }
        }

        private static byte[] ReadMemos(Stream stream)
        {
            if (stream == null)
            {
                throw new ArgumentNullException(nameof(stream));
            }

            using (MemoryStream ms = new MemoryStream())
            {
                stream.CopyTo(ms);
                return ms.ToArray();
            }
        }

        private void ReadHeader(BinaryReader reader)
        {
            // Peek at version number, then try to read correct version header.
            byte versionByte = reader.ReadByte();
            reader.BaseStream.Seek(0, SeekOrigin.Begin);
            DbfVersion version = (DbfVersion)versionByte;
            header = DbfHeader.CreateHeader(version);
            header.Read(reader);
        }

        private void ReadFields(BinaryReader reader)
        {
            Fields.Clear();

            // Fields are terminated by 0x0d char.
            while (reader.PeekChar() != 0x0d)
            {
                Fields.Add(new DbfField(reader, Encoding));
            }

            // Read fields terminator.
            reader.ReadByte();
        }

        private void ReadRecords(BinaryReader reader, byte[] memoData)
        {
            Records.Clear();

            // Records are terminated by 0x1a char (officially), or EOF (also seen).
            while (reader.PeekChar() != 0x1a && reader.PeekChar() != -1)
            {
                try
                {
                    Records.Add(new DbfRecord(reader, header, Fields, memoData, Encoding));
                }
                catch (EndOfStreamException) { }
            }
        }

        private void WriteFields(BinaryWriter writer)
        {
            foreach (DbfField field in Fields)
            {
                field.Write(writer, Encoding);
            }

            // Write field descriptor array terminator.
            writer.Write((byte)0x0d);
        }

        private void WriteRecords(BinaryWriter writer)
        {
            foreach (DbfRecord record in Records)
            {
                record.Write(writer, Encoding);
            }

            // Write EOF character.
            writer.Write((byte)0x1a);
        }

        private static string GetMemoPath(string basePath)
        {
            string memoPath = Path.ChangeExtension(basePath, "fpt");
            if (!File.Exists(memoPath))
            {
                memoPath = Path.ChangeExtension(basePath, "dbt");
                if (!File.Exists(memoPath))
                {
                    return null;
                }
            }
            return memoPath;
        }
    }
}
