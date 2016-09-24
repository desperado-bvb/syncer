package syncer

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/model"
)

type Schema struct {
	//todo: NameToID is nedd?
	schemaNameToID 	map[string]int64
	tableNameToID  	map[tableName]int64
	columnNameToID 	map[columnName]int64
	tableIDToName   map[int64] tableName

	schemas        	map[int64]*model.DBInfo
	tables         	map[int64]*model.TableInfo
	columns        	map[int64]*model.ColumnInfo

	schemaMetaVersion int64
}

type tableName struct {
	schema string
	table  string
}

type columnName struct {
	schema string
	table  string
	name   string
}

func NewSchema(store kv.Storage, ts uint64) (*Schema, error) {
	is := &Schema{}
	err := is.SyncTiDBSchema(store, ts)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return is, nil
}

func (s *Schema) SyncTiDBSchema(store kv.Storage, ts uint64) error {
	s.schemaNameToID = make(map[string]int64)
	s.tableNameToID = make(map[tableName]int64)
	s.columnNameToID = make(map[columnName]int64)
	s.tableIDToName  = make(map[int64]tableName)
	s.schemas = make(map[int64]*model.DBInfo)
	s.tables = make(map[int64]*model.TableInfo)
	s.columns = make(map[int64]*model.ColumnInfo)

	version := kv.NewVersion(ts)
	snapshot, err := store.GetSnapshot(version)
	if err != nil {
		return errors.Trace(err)
	}

	// sync all databases
	snapMeta := meta.NewSnapshotMeta(snapshot)
	dbs, err := snapMeta.ListDatabases()
	if err != nil {
		return errors.Trace(err)
	}

	for _, db := range dbs {
		// sync all tables under the databases
		tables, err := snapMeta.ListTables(db.ID)
		if err != nil {
			return errors.Trace(err)
		}

		db.Tables = tables
		s.schemaNameToID[db.Name.L] = db.ID
		s.schemas[db.ID] = db

		for _, table := range tables {
			s.tableNameToID[tableName{schema: db.Name.L, table: table.Name.L}] = table.ID
			s.tables[table.ID] = table
			s.tableIDToName[table.ID] = tableName{schema: db.Name.L, table: table.Name.L}

			for _, column := range table.Columns {
				s.columnNameToID[columnName{schema: db.Name.L, table: table.Name.L, name: column.Name.L}] = column.ID
				s.columns[column.ID] = column
			}
		}
	}

	schemeMetaVersion, err := snapMeta.GetSchemaVersion()
	if err != nil {
		return errors.Trace(err)
	}

	s.schemaMetaVersion = schemeMetaVersion
	return nil
}

func (s *Schema) SchemaMetaVersion() int64 {
	return s.schemaMetaVersion
}

func (s *Schema) SchemaByName(schema model.CIStr) (val *model.DBInfo, ok bool) {
	id, ok := s.schemaNameToID[schema.L]
	if !ok {
		return
	}

	val, ok = s.schemas[id]
	return
}

func (s *Schema) SchemaExists(schema model.CIStr) bool {
	_, ok := s.schemaNameToID[schema.L]
	return ok
}

func (s *Schema) TableByName(schema, table model.CIStr) (t *model.TableInfo, ok bool) {
	id, ok := s.tableNameToID[tableName{schema: schema.L, table: table.L}]
	if !ok {
		return
	}

	t = s.tables[id]
	return
}

func (s *Schema) TableExists(schema, table model.CIStr) bool {
	_, ok := s.tableNameToID[tableName{schema: schema.L, table: table.L}]
	return ok
}

func (s *Schema) ColumnByName(schema, table, name model.CIStr) (c *model.ColumnInfo, ok bool) {
	id, ok := s.columnNameToID[columnName{schema: schema.L, table: table.L, name: name.L}]
	if !ok {
		return
	}
	c = s.columns[id]
	return
}

func (s *Schema) ColumnExists(schema, table, name model.CIStr) bool {
	_, ok := s.columnNameToID[columnName{schema: schema.L, table: table.L, name: name.L}]
	return ok
}

func (s *Schema) TableNameByID(id int64) (val tableName, ok bool) {
	val, ok = s.tableIDToName[id]
	return
}

func (s *Schema) SchemaByID(id int64) (val *model.DBInfo, ok bool) {
	val, ok = s.schemas[id]
	return
}

func (s *Schema) TableByID(id int64) (val *model.TableInfo, ok bool) {
	val, ok = s.tables[id]
	return
}

func (s *Schema) ColumnByID(id int64) (val *model.ColumnInfo, ok bool) {
	val, ok = s.columns[id]
	return
}

func (s *Schema) DropSchema(schema model.CIStr) {
	id, ok := s.schemaNameToID[schema.L]
	if !ok {
		return
	}

	db, _ := s.schemas[id]
	for _, table := range db.Tables {
		tn := tableName{schema: db.Name.L, table: table.Name.L}
		id, ok = s.tableNameToID[tn]
		if !ok {
			continue
		}
		delete(s.tableNameToID, tn)
		delete(s.tableIDToName, id)
		delete(s.tables, id)
	}

	delete(s.schemaNameToID, schema.L)
	delete(s.schemas, id)

	return
}

func (s *Schema) CreateSchema(db *model.DBInfo) error {
	_, ok := s.schemaNameToID[db.Name.L]
	if ok {
		return errors.AlreadyExistsf("schema %s already exists", db.Name)
	}

	s.schemaNameToID[db.Name.L] = db.ID
	s.schemas[db.ID] = db

	return nil
}

func (s *Schema) DropTable(schema, table model.CIStr) {
	tn := tableName{schema: schema.L, table: table.L}
	id, ok := s.tableNameToID[tn]
	if !ok {
		return
	}

	delete(s.tableNameToID, tn)
	delete(s.tables, id)
	delete(s.tableIDToName, id)

	return
}

func (s *Schema) Createtable(schema model.CIStr, table *model.TableInfo) error {
	tn := tableName{schema: schema.L, table: table.Name.L}
	_, ok := s.tableNameToID[tn]
	if ok {
		return errors.AlreadyExistsf("table %s.%s already exists", schema, table.Name)
	}

	id, ok := s.schemaNameToID[schema.L]
	if !ok {
		return errors.NotFoundf("schema %s not found", schema)
	}

	s.schemas[id].Tables = append(s.schemas[id].Tables, table)

	for _, column := range table.Columns {
		s.columnNameToID[columnName{schema: schema.L, table: table.Name.L, name: column.Name.L}] = column.ID
		s.columns[column.ID] = column
	}

	s.tableNameToID[tn] = table.ID
	s.tableIDToName[table.ID] = tn
	s.tables[table.ID] = table

	return nil
}
