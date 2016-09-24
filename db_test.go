package syncer

import (
        "testing"

        . "github.com/pingcap/check"
	
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/types"
)

func TestClient(t *testing.T) {
        TestingT(t)
}

var _ = Suite(&testDBSuite{})

type testDBSuite struct{}

func (s *testDBSuite) TestGenInsertSQLs(c *C) {
	ms := &Mysql{}
	schema := "t"
	table := generateTestTable()
	
	colIDs := make([]int64, 0, 3)
	row := make([]types.Datum, 0, 3)
	var enum mysql.Enum	
	var err error

	for _, col := range table.Columns {
		if IsPKHandleColumn(table, col) {
			continue
		}
		colIDs = append(colIDs, col.ID)

		if col.ID == 2 {
			row  = append(row, types.NewDatum("liming"))
		} else if  col.ID == 3 {
			d := &types.Datum{}
			enum, err = mysql.ParseEnumName([]string{"female", "male"}, "male")
			c.Assert(err, IsNil)
			d.SetMysqlEnum(enum)
			row = append(row, *d)
		}
	}

	value, err := tablecodec.EncodeRow(row, colIDs)
        c.Assert(err, IsNil)
        handleVal, err := codec.EncodeValue(nil, types.NewIntDatum(1))
        c.Assert(err, IsNil)
        bin := append(handleVal, value...)
	
	sqls, vals, err := ms.genInsertSQLs(schema, table, [][]byte{bin})
	c.Assert(err, IsNil)
	if sqls[0] != "replace into t.account (id,name,male) values (?,?,?);" {
		c.Fatalf("insert sql %s , but want", sqls[0], "replace into t.account (id,name,male) values (?,?,?);")
	}

	valID, ok := vals[0][0].(int64)
	c.Assert(ok, Equals, true)
	valName, ok := vals[0][1].([]byte)
	c.Assert(ok, Equals, true)

	if valID != 1  || string(valName) != "liming"  {
		c.Fatalf("insert vals %v, but want  %d, %s, %d", vals[0], 1, []byte("liming"), 2)		
	}
}

func generateTestTable() *model.TableInfo {
	t := &model.TableInfo{}
	t.Name = model.NewCIStr("account")
	
	t.PKIsHandle = true
	userIDCol := &model.ColumnInfo {
                ID :  1,
		Name: model.NewCIStr("ID"),
		Offset: 0,
        }
	userIDCol.Flag = 2
	
	idIndex := &model.IndexInfo {
                Primary: true,
                Columns: []*model.IndexColumn{&model.IndexColumn{Offset:0}},
        }

	userNameCol := &model.ColumnInfo {
		ID: 2,		
		Name: model.NewCIStr("Name"),
		Offset:1,	
	}
	nameIndex := &model.IndexInfo {
		Primary: true,
		Columns: []*model.IndexColumn{&model.IndexColumn{Offset:0}},
	}

	t.Indices = []*model.IndexInfo{idIndex, nameIndex}
	
	maleCol := &model.ColumnInfo {
                ID: 3,
		Name: model.NewCIStr("male"),
		Offset:2,
        }

	t.Columns = []*model.ColumnInfo{userIDCol, userNameCol, maleCol}
	
	return t
}


