package db

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/model"
)

type opType int

const (
	insert = iota + 1
	update
	del
	delByID
	delByPK
	delByCol
	ddl
)

var providers = make(map[string]DB)

type DB interface {
	GenInsertSQLs(string, *model.TableInfo, [][]byte) ([]string, [][]interface{}, error)
	GenUpdateSQLs(string, *model.TableInfo, [][]byte) ([]string, [][]interface{}, error)
	GenDeleteSQLsByID(string, *model.TableInfo, []int64) ([]string, [][]interface{}, error)
	GenDeleteSQLs(string, *model.TableInfo, opType, [][]byte) ([]string, [][]interface{}, error)
	IsDDLSQL(string) (bool, error)
	GenDDLSQL(string, string) (string, error)
}

func Register(name string, provider DB) {
	if provider == nil {
		panic("db: Register provide is nil")
	}

	if _, dup := providers[name]; dup {
		panic("db: Register called twice for provider " + name)
	}

	providers[name] = provider
}

func Unregister(name string) {
	delete(providers, name)
}

type Manager struct {
	db DB
}

func NewManager(providerName string) (*Manager, error) {
	db, ok := providers[providerName]
	if !ok {
		return nil, errors.Errorf("db: unknown provider %q", providerName)
	}

	return &Manager{db: db}, nil
}

func (this *Manager) GenInsertSQLs(schema string, table *model.TableInfo, rows [][]byte) ([]string, [][]interface{}, error) {
	return this.db.GenInsertSQLs(schema, table, rows)
}

func (this *Manager) GenUpdateSQLs(schema string, table *model.TableInfo, rows [][]byte) ([]string, [][]interface{}, error) {
	return this.db.GenUpdateSQLs(schema, table, rows)
}

func (this *Manager) GenDeleteSQLsByID(schema string, table *model.TableInfo, rows []int64) ([]string, [][]interface{}, error) {
	return this.db.GenDeleteSQLsByID(schema, table, rows)
}

func (this *Manager) GenDeleteSQLs(schema string, table *model.TableInfo, op opType, rows [][]byte) ([]string, [][]interface{}, error) {
	return this.db.GenDeleteSQLs(schema, table, op, rows)
}

func (this *Manager) IsDDLSQL(sql string) (bool, error) {
	return this.db.IsDDLSQL(sql)
}

func (this *Manager) GenDDLSQL(sql string, schema string) (string, error) {
	return this.db.GenDDLSQL(sql, schema)
}
