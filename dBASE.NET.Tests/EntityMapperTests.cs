using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Aronic.dBASE.NET.Tests
{
    internal class TestEntity : IDbfBaseEntity
    {
        [DbfField]
        public int ID { get; set; }

        [DbfField("FULLNAME")]
        public string Name { get; set; }

        [DbfField("AGE")]
        public int Age { get; set; }

        public bool Something { get; set; }

        public bool IsDeleted { get; set; }
    }

    [TestClass]
    public class EntityMapperTests
    {
        internal Dbf CreateDbf()
        {
            var dbf = new Dbf(Encoding.GetEncoding(1250));
            var idField = new DbfField("ID", DbfFieldType.Integer, 4);
            var nameField = new DbfField("FULLNAME", DbfFieldType.Character, 50);
            var ageField = new DbfField("AGE", DbfFieldType.Integer, 4);
            dbf.Fields.Add(idField);
            dbf.Fields.Add(nameField);
            dbf.Fields.Add(ageField);
            return dbf;
        }

        [TestMethod]
        public void RecordFromEntity()
        {
            var entity = new TestEntity
            {
                ID = 1,
                Age = 30,
                Name = "John Doe",
                Something = true
            };

            var dbf = CreateDbf();

            var record = dbf.CreateRecord(entity);

            Assert.AreEqual(1, record["ID"]);
            Assert.AreEqual(30, record["AGE"]);
            Assert.AreEqual("John Doe", record["FULLNAME"]);
            Assert.AreEqual(null, record["Something"]);
            Assert.AreEqual(null, record["SOMETHING"]);
        }

        [TestMethod]
        public void EntityFromRecord()
        {
            var dbf = CreateDbf();
            var record = dbf.CreateRecord();
            record.Data[0] = 2;
            record.Data[1] = "Jane Doe";
            record.Data[2] = 25;

            var entity = new TestEntity();
            record.ToEntity(entity);

            Assert.AreEqual(2, entity.ID);
            Assert.AreEqual("Jane Doe", entity.Name);
            Assert.AreEqual(25, entity.Age);
        }

        [TestMethod]
        public void EntityFromDbf()
        {
            var dbf = new Dbf();
            dbf.Read("fixtures/mapper/mapper.dbf");
            var entites = new List<TestEntity>(dbf.GetEntities<TestEntity>());
            Assert.AreEqual(3, entites.Count);

            Assert.AreEqual(1, entites[0].ID);
            Assert.AreEqual("John Doe", entites[0].Name);
            Assert.AreEqual(30, entites[0].Age);
            Assert.AreEqual(false, entites[0].IsDeleted);

            Assert.AreEqual(2, entites[1].ID);
            Assert.AreEqual("Jane Doe", entites[1].Name);
            Assert.AreEqual(25, entites[1].Age);
            Assert.AreEqual(false, entites[1].IsDeleted);

            Assert.AreEqual(3, entites[2].ID);
            Assert.AreEqual("I'm removed", entites[2].Name);
            Assert.AreEqual(99, entites[2].Age);
            Assert.AreEqual(true, entites[2].IsDeleted);
        }

        [TestMethod]
        public void EntityToDbf()
        {
            var dbf = CreateDbf();
            var entities = new List<TestEntity>();
            entities.Add(new TestEntity
            {
                ID = 1,
                Age = 12,
                Name = "Bobby Doe",
                IsDeleted = false
            });

            entities.Add(new TestEntity
            {
                ID = 2,
                Age = 14,
                Name = "Stacy Doe",
                IsDeleted = true
            });
            dbf.AddEntities(entities);
            dbf.Write("mapper-test.dbf");

            var check = new Dbf();
            check.Read("mapper-test.dbf");
            var entitiesFromDBf = new List<TestEntity>(check.GetEntities<TestEntity>());

            Assert.AreEqual(2, entitiesFromDBf.Count);

            Assert.AreEqual(1, entitiesFromDBf[0].ID);
            Assert.AreEqual("Bobby Doe", entitiesFromDBf[0].Name);
            Assert.AreEqual(12, entitiesFromDBf[0].Age);
            Assert.AreEqual(false, entitiesFromDBf[0].IsDeleted);

            Assert.AreEqual(2, entitiesFromDBf[1].ID);
            Assert.AreEqual("Stacy Doe", entitiesFromDBf[1].Name);
            Assert.AreEqual(14, entitiesFromDBf[1].Age);
            Assert.AreEqual(true, entitiesFromDBf[1].IsDeleted);
        }

        [TestMethod]
        public async Task AsyncToEntity()
        {
            var dbf = new Dbf();
            dbf.Fields.Add(new DbfField("ID", DbfFieldType.Integer, 4));
            dbf.Fields.Add(new DbfField("FULLNAME", DbfFieldType.Character, 25));
            dbf.Fields.Add(new DbfField("AGE", DbfFieldType.Integer, 4));

            var numberOfEntities = 25_000;
            for(int i = 0; i < numberOfEntities; i++)
            {
                var record = dbf.CreateRecord();
                record.Data[0] = i;
                record.Data[1] = Guid.NewGuid().ToString();
                record.Data[2] = i;
            }

            var sw = new Stopwatch();
            sw.Start();
            var entites = await dbf.GetEntitiesAsync<TestEntity>();
            sw.Stop();
            var mtTime = sw.ElapsedMilliseconds;
            Assert.AreEqual(numberOfEntities, entites.Count());
            Assert.IsFalse(entites.Any(x => x == null));

            sw.Reset();
            sw.Start();
            var entities2 = dbf.GetEntities<TestEntity>();
            sw.Stop();
            var stTime = sw.ElapsedMilliseconds;

            Console.WriteLine($"old:{stTime} new:{mtTime}");
            Assert.IsTrue(mtTime < stTime);
        }
    }
}
