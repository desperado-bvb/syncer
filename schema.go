package syncer

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/model"
)

type infoSchema struct {
	schemaNameToID 	map[string]int64
	tableNameToID  	map[tableName]int64
	columnNameToID 	map[columnName]int64
	TableIDToName   map[int64] tableName
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
	tableName
	name string
}

func NewInfoSchema(store kv.Storage) (*infoSchema, error) {
	is := &infoSchema{}
	err := is.SyncTiDBSchema(store)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return is, nil
}

func (is *infoSchema) SyncTiDBSchema(store kv.Storage) error {
	is.schemaNameToID = make(map[string]int64)
	is.tableNameToID = make(map[tableName]int64)
	is.columnNameToID = make(map[columnName]int64)
	is.TableIDToName  = make(map[int64]tableName)
	is.schemas = make(map[int64]*model.DBInfo)
	is.tables = make(map[int64]*model.TableInfo)
	is.columns = make(map[int64]*model.ColumnInfo)

	version, err := store.CurrentVersion()
	if err != nil {
		return errors.Trace(err)
	}

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
		is.schemaNameToID[db.Name.L] = db.ID
		is.schemas[db.ID] = db

		for _, table := range tables {
			is.tableNameToID[tableName{schema: db.Name.L, table: table.Name.L}] = table.ID
			is.tables[table.ID] = table
			is.TableIDToName[table.ID] = tableName{schema: db.Name.L, table: table.Name.L}

			for _, column := range table.Columns {
				is.columnNameToID[columnName{schema: db.Name.L, table: table.Name.L, name: column.Name.L}] = column.ID
				is.columns[column.ID] = column
			}
		}
	}

	schemeMetaVersion, err := snapMeta.GetSchemaVersion()
	if err != nil {
		return errors.Trace(err)
	}

	is.schemaMetaVersion = schemeMetaVersion
	return nil
}

func (is *infoSchema) SchemaMetaVersion() int64 {
	return is.schemaMetaVersion
}

func (is *infoSchema) SchemaByName(schema model.CIStr) (val *model.DBInfo, ok bool) {
	id, ok := is.schemaNameToID[schema.L]
	if !ok {
		return
	}

	val, ok = is.schemas[id]
	return
}

func (is *infoSchema) SchemaExists(schema model.CIStr) bool {
	_, ok := is.schemaNameToID[schema.L]
	return ok
}

func (is *infoSchema) TableByName(schema, table model.CIStr) (t *model.TableInfo, ok bool) {
	id, ok := is.tableNameToID[tableName{schema: schema.L, table: table.L}]
	if !ok {
		return
	}

	t = is.tables[id]
	return
}

func (is *infoSchema) TableExists(schema, table model.CIStr) bool {
	_, ok := is.tableNameToID[tableName{schema: schema.L, table: table.L}]
	return ok
}

func (is *infoSchema) ColumnByName(schema, table, name model.CIStr) (c *model.ColumnInfo, ok bool) {
	id, ok := is.columnNameToID[tableName{schema: schema.L, table: table.L, name: name.L}]
	if !ok {
		return
	}
	c = is.columns[id]
	return
}

func (is *infoSchema) ColumnExists(schema, table, name model.CIStr) bool {
	_, ok := is.columnNameToID[tableName{schema: schema.L, table: table.L, name: name.L}]
	return ok
}

func (is *infoSchema) TableNamebyID(id int64) (val tableName, ok bool) {
	val, ok = is.tableIDToName[id]
	return
}

func (is *infoSchema) SchemaByID(id int64) (val *model.DBInfo, ok bool) {
	val, ok = is.schemas[id]
	return
}

func (is *infoSchema) TableByID(id int64) (val *model.TableInfo, ok bool) {
	val, ok = is.tables[id]
	return
}

func (is *infoSchema) ColumnByID(id int64) (val *model.ColumnInfo, ok bool) {
	val, ok = is.columns[id]
	return
}

func (is *infoSchema) SchemaTables(schema model.CIStr) (tables []*model.TableInfo) {
	di, ok := is.SchemaByName(schema)
	if !ok {
		return
	}
	for _, ti := range di.Tables {
		tables = append(tables, is.tables[ti.ID])
	}
	return
}

func (is *infoSchema) DropSchema(schema model.CIStr) {
	id, ok := is.schemaNameToID[schema.L]
	if !ok {
		return
	}

	db, _ := is.schemas[id]
	for _, table := range db.Tables {
		tn := tableName{schema: db.Name.L, table: table.Name.L}
		id, ok = is.tableNameToID[tn]
		if !ok {
			continue
		}
		delete(is.tableNameToID, tn)
		delete(is.tables, id)
	}

	delete(is.schemaNameToID, schema.L)
	delete(is.schemas, id)

	return
}

func (is *infoSchema) CreateSchema(db *model.DBInfo) error {
	_, ok := is.schemaNameToID[db.Name.L]
	if ok {
		return errors.AlreadyExistsf("schema %s already exists", db.Name)
	}

	schemaNameToID[db.Name.L] = db.ID
	schemas[db.ID] = db
}

func (is *infoSchema) DropTable(schema, table model.CIStr) {
	tn := tableName{schema: schema.L, table: table.L}
	id, ok = is.tableNameToID[tn]
	if !ok {
		return
	}

	delete(is.tableNameToID, tn)
	delete(is.tables, id)

	return
}

func (is *infoSchema) Createtable(schema model.CIStr, table *model.TableInfo) error {
	tn := tableName{schema: schema.L, table: table.Name.L}
	_, ok := is.tableNameToID[tn]
	if ok {
		return errors.AlreadyExistsf("table %s.%s already exists", schema, table.Name)
	}

	id, ok := is.schemaNameToID[scheme.L]
	if !ok {
		return errors.NotFoundf("schema %s not found", schema)
	}

	db, _ := is.Schemas[id]
	db.Tables = append(db.Tables, table)

	for _, column := range table.Columns {
		is.columnNameToID[columnName{schema: shcema.L, table: table.Name.L, name: column.Name.L}] = column.ID
		is.columns[column.ID] = column
	}

	is.tableNameToID[tn] = table.ID
	is.tableIDToName[table.ID] = tn
	is.tables[table.ID] = table
}

func (is *infoSchema) RemoveColumn(schema, table, name model.CIStr) error {
	cn := columnName{schema: schema.L, table: table.L, name: name.L}
	id, ok = is.columnNameToID[cn]
	if !ok {
		return
	}

	tn := tableName{schema: schema.L, table: table.L}
	tableID, ok = is.tableNameToID[tn]
	if !ok {
		return errors.NotFoundf("table %s.%s not found", schema, table)
	}

	tableInfo := is.tables[tableID]
	for i, column := range tableInfo.Columns {
		if column.Name.L == name.L {
			length := len(tableInfo.Columns)
			if length == 1 {
				is.DropTable(schema, table)
			}

			copy(tableInfo.Columns[i:], tableInfo.Columns[i+1:])
			tableInfo.Columns = tableInfo.Columns[:length-1]
			break
		}
	}

	delete(is.columnNameToID, cn)
	delete(is.columns, id)
}

func (is *infoSchema) AddColumn(schema, table model.CIStr, column *model.Column) error {
	cn := columnName{schema: schema.L, table: table.L, name: column.Name.L}
	id, ok = is.columnNameToID[cn]
	if ok {
		return errors.AlreadyExistsf("table %s.%s column %s already exists", schema, table, column.Name)
	}

	tn := tableName{schema: schema.L, table: table.L}
	tableID, ok := is.tableNameToID[tn]
	if ok {
		return errors.NotFoundf("table %s.%s not found", schema, table)
	}

	tableInfo = is.tables[tableID]
	tableInfo.Columns = append(tableInfo.Columns, column)

	is.columnNameToID[cn] = column.ID
	is.columns[column.ID] = column

	return nil
}
