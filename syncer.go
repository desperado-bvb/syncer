package syncer

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/model"
	pb "github.com/pingcap/tipb/go-binlog"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv"
)

type Syncer struct {
	sync.Mutex

	cfg *Config

	meta *Schema

	db *Mysql

	wg    sync.WaitGroup
	jobWg sync.WaitGroup

	input []chan interface{}

	store *kv.Storage
	toDB  *sql.DB

	done chan struct{}
	jobs []chan *job

}

func NewSyncer(cfg *Config) *Syncer {
	syncer := new(Syncer)
	syncer.cfg = cfg
	syncer.db = new Mysql()

	kv := tikv.Driver{}        
	s := cfg.DbName    
	p := cfg.DbHost        
	path := fmt.Sprintf("%s://%s", s, p)        
	syncer.store, err := kv.Open(path)
	if err != nil {                
		stop(err)        
	}        
	defer syncer.store.Close()

	syncer.meta = NewSchema(syncer.db,cfg.KsTime)
	syncer.done = make(chan struct{})
	syncer.jobs = newJobChans(cfg.WorkerCount)
	return syncer
}

func (s *Syncer) getSchemaInfo(id int64, sql string) (string, error) {
	job, err := s.getHistoryJob(id)
	if err != nil {
		return nil, errors.Trace(err)
	}

	fo job.SchemaState != model.StatePublic {
		time.Sleep(10*time.Second)
		job, err = s.getHistoryJob(id)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	stmt, err := parser.New().ParseOneStmt(sql, "", "")
	if err != nil {
		return nil, errors.Trace(err)
	}

	switch stmt.(type) {
	case *ast.CreateDatabaseStmt:
		
	case *ast.DropDatabaseStmt:
		schemaID := job.SchemaID
		schema, ok := s.meta.SchemaByID(schemaID)
		if !ok {
			return nil, errors.NotFoundf("schema %d", schemaID)
		}

		s.meta.DropSchema(schema.Name)

	case *ast.CreateTableStmt:
	case *ast.DropTableStmt:
		tableID := job.TableID
                table, ok := s.meta.TableByID(tableID)
                if !ok {
                        return nil, errors.NotFoundf("table %d", tabelID)
                }

		tableName := s.meta.TableNamebyID(tableID)
                s.meta.DropSchema(tableName)

	case *ast.AlterTableSpec:
	}
	
}

func (s *Syncer) getHistoryJob(id int64) err {
	version, err := s.store.CurrentVersion()
        if err != nil {
                return nil, errors.Trace(err)
        }

        snapshot, err := s.store.GetSnapshot(version)
        if err != nil {
                return nil, errors.Trace(err)
        }

        snapMeta := meta.NewSnapshotMeta(snapshot)
        job, err := snapMeta.GetHistoryDDLJob(id)
        if err != nil {
                return nil, errors.Trace(err)
        }

	return job, err
}

func (s *Syncer) run() error {
	defer s.wg.Done()

	//todb: input
	for {
		rawBinlog := <- s.Input
		
		binlog :=  &pb.Binlog{}
		err := binlog.Unmarshal(rawBinlog)
		if err != nil {
			return errors.Errorf("binlog %s unmarshal error %v", rawBinlog, err)
		}

		switch binlog.GetTp() {
		case binlog.BinlogType_Prewrite:
			preWriteValue := binlog.GetPrewriteValue()
			preWrite := &pb.PrewriteValue{}
			err = preWrite.Unmarshal(preWriteValue)
			if err != nil {
				return errors.Errorf("prewrite %s unmarshal error %v", preWriteValue, err)
			}

			mutations := preWrite.GetMutations
			table, ok := s.meta.TableByID(mutations.GetTableId())
			if !ok {
				continue
			}

			tn, ok := s.meta.TableNamebyID(utations.GetTableId())
			if !ok {
				continue
			}

			//todb: get schema api
			schema := tn.schema
			
			var (
				sqls []string
				args [][]interface{}
			)

			for _, mutation := range mutations {
				if len(mutation.GetInsertedRows()) > 0 {
					sql, arg, err := s.db.(schema, table, mutation.GetInsertedRows())
					if err != nil {
						return errors.Errorf("gen insert sqls failed: %v, schema: %s, table: %s", err, schema, table.Name)
					}

					sqls = append(sqls, sql...)
					args = append(sqls, arg...)
				}

				if len(mutation.GetUpdatedRows()) > 0 {
					sql, arg, err := s.db.genUpdateSQLs(schema, table, mutation.GetUpdatedRows())
					if err != nil {
                                                return errors.Errorf("gen update sqls failed: %v, schema: %s, table: %s", err, schema, table.Name)
                                        }

                                        sqls = append(sqls, sql...)
                                        args = append(sqls, arg...)
				}

				if len(mutation.GetDeletedIds()) > 0 {
					sql, arg, err := s.db.genDeleteSQLs(shcme, table, delID, mutation.GetDeletedIds())
					if err != nil {
                                                return errors.Errorf("gen delete sqls failed: %v, schema: %s, table: %s", err, schema, table.Name)
                                        }

                                        sqls = append(sqls, sql...)
                                        args = append(sqls, arg...)
				}

				if len(mutation.GetDeletedPks()) > 0 {
					sql, arg, err := s.db.genDeleteSQLs(shcme, table, delPK, mutation.GetDeletedPks())
					if err != nil {
                                                return errors.Errorf("gen delete sqls failed: %v, schema: %s, table: %s", err, schema, table.Name)
                                        }

                                        sqls = append(sqls, sql...)
                                        args = append(sqls, arg...)
				}

				if len(mutation.GetDeletedRows()) > 0 {
					s.db.genDeleteSQLs(shcme, table, delCol, mutation.GetDeletedRows())
					if err != nil {
                                                return errors.Errorf("gen delete sqls failed: %v, schema: %s, table: %s", err, schema, table.Name)
                                        }

                                        sqls = append(sqls, sql...)
                                        args = append(sqls, arg...)
				}
				
				for i := range sqls {
                                	job := newJob(insert, sqls[i], args[i], true)
                                        err = s.addJob(job)
                                        if err != nil {
                                        	return errors.Trace(err)
                                        }
				}
			
			}
			
		case binlog.BinlogType_PreDDL:
			ok := false
			sql := string(binlog.GetDdlQuery())
			jobID := binlog.GetDdlJobId()
			
			schema, err := s.syncSchemaInfo(jobID)
			if err != nil {
				return errors.Trace(err)
			}

			tn, ok := s.meta.TableNamebyID(tableID)
                        if !ok {
                                return errors.Errorf("the syncer scheme is wrong, not found table ID %d", tableID)
                        }

                        //todb: get schema api
                        schema := tn.schema

			log.Debugf("[query]%s", sql)

			ok, err = s.db.isDDLSQL(sql)
			if err != nil {
				return errors.Errorf("parse query event failed: %v", err)
			}

			if ok {
				
				sql, err = s.db.genDDLSQL(sql, schema)
				if err != nil {
					return errors.Trace(err)
				}

				log.Infof("[ddl][start]%s[pos]%v[next pos]%v", sql, lastPos, pos)

				job := newJob(ddl, sql, nil,  false, pos)
				err = s.addJob(job)
				if err != nil {
					return errors.Trace(err)
				}

				log.Infof("[ddl][end]%s[pos]%v[next pos]%v", sql, lastPos, pos)
			}			
		} 
	}
}
