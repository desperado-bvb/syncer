package syncer

import (
	"fmt"
	"strings"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/types"
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

type Mysql struct{}

func (m *Mysql) genInsertSQLs(schema string, table *model.TableInfo, rows [][]byte) ([]string, [][]interface{}, error) {
	columns := table.Columns
	sqls := make([]string, 0, len(rows))
	values := make([][]interface{}, 0, len(rows))

	columnList := genColumnList(columns)
	columnPlaceholders := genColumnPlaceholders((len(columns)))
	sql := fmt.Sprintf("replace into %s.%s (%s) values (%s);", schema, table.Name, columnList, columnPlaceholders)

	for _, row := range rows {
		remain, pk, err := codec.DecodeOne(row)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}

		r, err := codec.Decode(remain, 2*(len(columns)-1))
		if err != nil {
			return nil, nil, errors.Trace(err)
		}

		if len(r)%2 != 0 {
			return nil, nil, errors.Errorf("table %s.%s insert row raw data is corruption %v", schema, table.Name, r)
		}

		var columnValues = make(map[int64]types.Datum)
		for i := 0; i < len(r); i += 2 {
			columnValues[r[i].GetInt64()] = r[i+1]
		}

		var vals []interface{}
		for _, col := range columns {
			if IsPKHandleColumn(table, col) {
				vals = append(vals, pk.GetValue())
				continue
			}

			val, ok := columnValues[col.ID]
			if !ok {
				vals = append(vals, col.DefaultValue)
			} else {
				vals = append(vals, val.GetValue())
			}
		}

		sqls = append(sqls, sql)
		values = append(values, vals)
	}

	return sqls, values, nil
}

func (m *Mysql) genUpdateSQLs(schema string, table *model.TableInfo, rows [][]byte) ([]string, [][]interface{}, error) {
	length := len(rows)
	columns := table.Columns
	sqls := make([]string, 0, length)
	values := make([][]interface{}, 0, length)

	for _, row := range rows {
		var updateColumns []*model.ColumnInfo
		var oldValues []interface{}
		var newValues []interface{}

		pc := pkColumn(table)
		if pc != nil {
			updateColumns = append(updateColumns, pc)

			remain, pk, err := codec.DecodeOne(row)
			if err != nil {
				return nil, nil, errors.Trace(err)
			}
			row = remain

			oldValues = append(oldValues, pk.GetValue())
		}

		r, err := codec.Decode(row, length-1)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}

		var i int
		if pc == nil {
			if len(r)%2 != 0 {
				return nil, nil, errors.Errorf("table %s.%s update row data is corruption %v", schema, table.Name, r)
			}

			columnValues := make(map[int64]types.Datum)
			for ; i < len(r)/2; i += 2 {
				columnValues[r[i].GetInt64()] = r[i+1]
			}

			for _, col := range columns {
				val, ok := columnValues[col.ID]
				if ok {
					updateColumns = append(updateColumns, col)
					oldValues = append(oldValues, val.GetValue())
				}
			}
		}

		whereColumns := updateColumns
		columnValues := make(map[int64]types.Datum)
		updateColumns = nil

		for ; i < len(r); i += 2 {
			columnValues[r[i].GetInt64()] = r[i+1]
		}

		for _, col := range columns {
			val, ok := columnValues[col.ID]
			if ok {
				updateColumns = append(updateColumns, col)
				newValues = append(newValues, val.GetValue())
			}
		}

		var value []interface{}
		kvs := genKVs(updateColumns)
		value = append(value, newValues...)
		value = append(value, oldValues...)

		where := genWhere(whereColumns, oldValues)
		sql := fmt.Sprintf("update %s.%s set %s where %s limit 1;", schema, table.Name.L, kvs, where)
		sqls = append(sqls, sql)
		values = append(values, value)
	}

	return sqls, values, nil
}

func (s *Mysql) genDeleteSQLsByID(schema string, table *model.TableInfo, rows []int64) ([]string, [][]interface{}, error) {
	sqls := make([]string, 0, len(rows))
	values := make([][]interface{}, 0, len(rows))
	column := pkColumn(table)
	if column == nil {
		return nil, nil, errors.Errorf("table %s.%s dont have pkHandle column", schema, table.Name)
	}

	whereColumns := []*model.ColumnInfo{column}

	for _, rowID := range rows {
		var value []interface{}
		value = append(value, rowID)

		where := genWhere(whereColumns, value)
		values = append(values, value)

		sql := fmt.Sprintf("delete from %s.%s where %s limit 1;", schema, table.Name, where)
		sqls = append(sqls, sql)
	}

	return sqls, values, nil

}

func (s *Mysql) genDeleteSQLs(schema string, table *model.TableInfo, op opType, rows [][]byte) ([]string, [][]interface{}, error) {
	columns := table.Columns
	sqls := make([]string, 0, len(rows))
	values := make([][]interface{}, 0, len(rows))

	for _, row := range rows {
		var whereColumns []*model.ColumnInfo
		var value []interface{}
		r, err := codec.Decode(row, len(columns))
		if err != nil {
			return nil, nil, errors.Trace(err)
		}

		switch op {
		case delByPK:
			whereColumns = pksColumns(table)
			if whereColumns == nil {
				return nil, nil, errors.Errorf("table %s.%s dont have pkHandle column", schema, table.Name)
			}

			if len(r) != len(whereColumns) {
				return nil, nil, errors.Errorf("table %s.%s the delete row by pks binlog %v is courruption", schema, table.Name, r)
			}

			for _, val := range r {
				value = append(value, val.GetValue())
			}

		case delByCol:
			whereColumns = columns

			if len(r)/2 != len(whereColumns) {
				return nil, nil, errors.Errorf("table %s.%s the delete row by cols binlog %v is courruption", schema, table.Name, r)
			}

			var columnValues = make(map[int64]types.Datum)
			for i := 0; i < len(r); i += 2 {
				columnValues[r[i].GetInt64()] = r[i+1]
			}

			for _, col := range columns {
				val, ok := columnValues[col.ID]
				if ok {
					value = append(value, val.GetValue())
				}
			}
		default:
			return nil, nil, errors.Errorf("delete row error type %v", op)
		}

		where := genWhere(whereColumns, value)
		values = append(values, value)

		sql := fmt.Sprintf("delete from %s.%s where %s limit 1;", schema, table.Name, where)
		sqls = append(sqls, sql)
	}

	return sqls, values, nil
}

func (s *Mysql) isDDLSQL(sql string) (bool, error) {
	stmt, err := parser.New().ParseOneStmt(sql, "", "")
	if err != nil {
		return false, errors.Errorf("[sql]%s[error]%v", sql, err)
	}

	_, isDDL := stmt.(ast.DDLNode)
	return isDDL, nil
}

//todo: check ddl query contains schema
func (s *Mysql) genDDLSQL(sql string, schema string) (string, error) {
	stmt, err := parser.New().ParseOneStmt(sql, "", "")
	if err != nil {
		return "", errors.Trace(err)
	}

	_, isCreateDatabase := stmt.(*ast.CreateDatabaseStmt)
	if isCreateDatabase {
		return fmt.Sprintf("%s;", sql), nil
	}

	return fmt.Sprintf("use %s; %s;", schema, sql), nil
}

func genColumnList(columns []*model.ColumnInfo) string {
	var columnList []byte
	for i, column := range columns {
		columnList = append(columnList, []byte(column.Name.L)...)

		if i != len(columns)-1 {
			columnList = append(columnList, ',')
		}
	}

	return string(columnList)
}

func genColumnPlaceholders(length int) string {
	values := make([]string, length, length)
	for i := 0; i < length; i++ {
		values[i] = "?"
	}
	return strings.Join(values, ",")
}

func genKVs(columns []*model.ColumnInfo) string {
	var kvs []byte
	for i := range columns {
		if i == len(columns)-1 {
			kvs = append(kvs, []byte(fmt.Sprintf("%s = ?", columns[i].Name))...)
		} else {
			kvs = append(kvs, []byte(fmt.Sprintf("%s = ?, ", columns[i].Name))...)
		}
	}

	return string(kvs)
}

func genWhere(columns []*model.ColumnInfo, data []interface{}) string {
	var kvs []byte
	for i := range columns {
		kvSplit := "="
		if data[i] == nil {
			kvSplit = "is"
		}

		if i == len(columns)-1 {
			kvs = append(kvs, []byte(fmt.Sprintf("%s %s ?", columns[i].Name, kvSplit))...)
		} else {
			kvs = append(kvs, []byte(fmt.Sprintf("%s %s ? and ", columns[i].Name, kvSplit))...)
		}
	}

	return string(kvs)
}

func pkColumn(table *model.TableInfo) *model.ColumnInfo {
	for _, col := range table.Columns {
		if IsPKHandleColumn(table, col) {
			return col
		}
	}

	return nil
}

func pksColumns(table *model.TableInfo) []*model.ColumnInfo {
	for _, idx := range table.Indices {
		if idx.Primary {
			var cols []*model.ColumnInfo
			columns := make(map[string]byte)
			for _, col := range idx.Columns {
				columns[col.Name.L] = 0x01
			}

			for _, col := range table.Columns {
				if _, ok := columns[col.Name.L]; ok {
					cols = append(cols, col)
				}
			}

			return cols
		}
	}

	return nil
}

func IsPKHandleColumn(table *model.TableInfo, column *model.ColumnInfo) bool {
	return mysql.HasPriKeyFlag(column.Flag) && table.PKIsHandle
}
