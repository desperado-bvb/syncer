package syncer

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
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

	syncer *syncTiDBSchema

	input []chan interface{}
	pos *mysql.Position

	store *kv.Storage
	toDB  *sql.DB

	done chan struct{}
	jobs chan *job

}

func NewSyncer(cfg *Config) *Syncer {
	syncer := new(Syncer)
	syncer.cfg = cfg
	syncer.db = new Mysql()
	syncer.meta = NewSchema(syncer.db,cfg.KsTime) // err
	syncer.done = make(chan struct{})
	syncer.jobs = newJobChans(cfg.WorkerCount)
	return syncer
}

func newJobChans(count int) []chan *job {
	jobs := make([]chan *job, 0, count)
	for i := 0; i < count; i++ {
		jobs = append(jobs, make(chan *job, 1000))
	}

	return jobs
}

func closeJobChans(jobs []chan *job) {
	for _, ch := range jobs {
		close(ch)
	}
}

func (s *Syncer) addJob(job *job) error {
	s.jobWg.Add(1)

	s.jobs <- job

	wait := s.checkWait(job)
	if wait {
		s.jobWg.Wait()

		err := s.meta.Save(job.pos)
		if err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

// Start starts syncer.
func (s *Syncer) Start() error {
	err := s.meta.Load()
	if err != nil {
		return errors.Trace(err)
	}

	s.wg.Add(1)

	err = s.run()
	if err != nil {
		return errors.Trace(err)
	}

	s.done <- struct{}{}

	return nil
}

func (s *Syncer) checkBinlogFormat() error {
	rows, err := s.fromDB.Query(`SHOW GLOBAL VARIABLES LIKE "binlog_format";`)
	if err != nil {
		return errors.Trace(err)
	}
	defer rows.Close()

	// Show an example.
	/*
		mysql> SHOW GLOBAL VARIABLES LIKE "binlog_format";
		+---------------+-------+
		| Variable_name | Value |
		+---------------+-------+
		| binlog_format | ROW   |
		+---------------+-------+
	*/
	for rows.Next() {
		var (
			variable string
			value    string
		)

		err = rows.Scan(&variable, &value)
		if err != nil {
			return errors.Trace(err)
		}
	}

	if rows.Err() != nil {
		return errors.Trace(rows.Err())
	}

	return nil
}

func (s *Syncer) getSchemaInfo(id int64, sql string) (string, error) {
	job, err := s.getHistoryJob(id)
	if err != nil {
		return nil, errors.Trace(err)
	}

	for job.SchemaState != model.StatePublic {
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
		// todo: check the version can be peek by job.SnapshotVer
		version := kv.NewVersion(job.SnapshotVer)
    		snapshot, err := store.GetSnapshot(version)
		if err != nil {
			return nil, errors.Trace(err)
		}

    		snapMeta := meta.NewSnapshotMeta(snapshot)
		db, err := snapMeta.GetDatabase(job.SchemaID)
		if err != nil {
			return nil, errors.Trace(err)
		}

		d, ok := s.meta.schemaByID(job.SchemaID)
                if ok {
                        return nil, errors.AlreadyExistsf("schema %d", job.SchemaID)
                }

		s.meta.CreateSchema(db)
		return db.Name.L, nil
		
	case *ast.DropDatabaseStmt:
		schemaID := job.SchemaID
		schema, ok := s.meta.SchemaByID(schemaID)
		if !ok {
			return nil, errors.NotFoundf("schema %d", schemaID)
		}

		s.meta.DropSchema(schema.Name)
		return schema.Name.L, nil

	case *ast.CreateTableStmt:
		version := kv.NewVersion(job.SnapshotVer)
                snapshot, err := store.GetSnapshot(version)
                if err != nil {
                        return nil, errors.Trace(err)
                }

		//todb: check job is must with SchemaID, TableID
		snapMeta := meta.NewSnapshotMeta(snapshot)
		table, err := snapMeta.GetTable(job.SchemaID, job.TableID)
		if err != nil {
			return nil, errors.Trace(err)
		}

		db, ok := s.meta.schemaByID(job.SchemaID)
		if !ok {
			return nil, errors.NotFoundf("schema %d", schemaID)
		}

		tb, ok := s.meta.tableByID(job.TableID)
		if ok {
			return nil, errors.AlreadyExistsf("table %d", job.TableID)
		}

		s.meta.CreateTable(db.Name, table)
		return db.Name, nil

	case *ast.DropTableStmt:
		tableID := job.TableID
		dbID := job.SchemaID

		db, ok := s.meta.schemaByID(job.SchemaID)
                if !ok {
			return nil, errors.NotFoundf("schema %d", schemaID)
		}

                table, ok := s.meta.TableByID(tableID)
                if !ok {
                        return nil, errors.NotFoundf("table %d", tabelID)
                }

                s.meta.DropTable(db.Name, table.Name)
		return db.Name.L, nil

	case *ast.AlterTableSpec:
		version := kv.NewVersion(job.SnapshotVer)
                snapshot, err := store.GetSnapshot(version)
                if err != nil {
                        return nil, errors.Trace(err)
                }

                snapMeta := meta.NewSnapshotMeta(snapshot)
                table, err := snapMeta.GetTable(job.SchemaID, job.TableID)
                if err != nil {
                        return nil, errors.Trace(err)
                }

                db, ok := s.meta.schemaByID(job.SchemaID)
                if !ok {
                        return nil, errors.NotFoundf("schema %d", schemaID)
                }

                tb, ok := s.meta.tableByID(job.TableID)
                if !ok {
                        return nil, errors.NotFoundf("table %d", job.TableID)
                }

		s.meta.DropTable(db.Name, tb.Name)
		s.meta.Createtable(db.Name, table)
	
		return db.Name.L, nil
	
	defaut:
		version := kv.NewVersion(job.SnapshotVer)
                snapshot, err := store.GetSnapshot(version)
                if err != nil {
                        return nil, errors.Trace(err)
                }

                snapMeta := meta.NewSnapshotMeta(snapshot)
                db, err := snapMeta.GetDatabase(job.SchemaID)
                if err != nil {
                        return nil, errors.Trace(err)
                }
		
		return 	db.Name.L, nil
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

func (s *Syncer) Pos() mysql.Position {
	s.RLock()
	defer s.RUnlock()// err isthis lock right?

	return mysql.Position{Name: s.BinLogName, Pos: s.BinLogPos}
}

func (s *Syncer) printStatus() {
	defer s.wg.Done()

	timer := time.NewTicker(statusTime)
	defer timer.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-timer.C:
			now := time.Now()
			seconds := now.Unix() - s.lastTime.Unix()
			totalSeconds := now.Unix() - s.start.Unix()
			last := s.lastCount.Get()
			total := s.count.Get()

			tps, totalTps := int64(0), int64(0)
			if seconds > 0 {
				tps = (total - last) / seconds
				totalTps = total / totalSeconds
			}

			log.Infof("[syncer]total events = %d, insert = %d, update = %d, delete = %d, total tps = %d, recent tps = %d, %s.",
				total, s.insertCount.Get(), s.updateCount.Get(), s.deleteCount.Get(), totalTps, tps, s.meta)

			s.lastCount.Set(total)
			s.lastTime = time.Now()
		}
	}
}

func (s *Syncer) sync(db *sql.DB, jobChan chan *job) {
	defer s.wg.Done()

	idx := 0
	count := s.cfg.Batch
	sqls := make([]string, 0, count)
	args := make([][]interface{}, 0, count)
	lastSyncTime := time.Now()

	var err error
	for {
		select {
		case job, ok := <-jobChan:
			if !ok {
				return
			}

			idx++

			if job.tp == ddl {
				err = executeSQL(db, sqls, args, true)
				if err != nil {
					log.Fatalf(errors.ErrorStack(err))
				}

				err = executeSQL(db, []string{job.sql}, [][]interface{}{job.args}, false)
				if err != nil {
					if !ignoreDDLError(err) {
						log.Fatalf(errors.ErrorStack(err))
					} else {
						log.Warnf("[ignore ddl error][sql]%s[args]%v[error]%v", job.sql, job.args, err)
					}
				}

				idx = 0
				sqls = sqls[0:0]
				args = args[0:0]
				lastSyncTime = time.Now()
			} else {
				sqls = append(sqls, job.sql)
				args = append(args, job.args)
			}

			if idx >= count {
				err = executeSQL(db, sqls, args, true)
				if err != nil {
					log.Fatalf(errors.ErrorStack(err))
				}

				idx = 0
				sqls = sqls[0:0]
				args = args[0:0]
				lastSyncTime = time.Now()
			}

			s.addCount(job.tp)
			s.jobWg.Done()
		default:
			now := time.Now()
			if now.Sub(lastSyncTime) >= maxWaitTime {
				err = executeSQL(db, sqls, args, true)
				if err != nil {
					log.Fatalf(errors.ErrorStack(err))
				}

				idx = 0
				sqls = sqls[0:0]
				args = args[0:0]
				lastSyncTime = now
			}

			time.Sleep(waitTime)
		}
	}
}

func (s *Syncer) run() error {
	kv := tikv.Driver{}
	path := fmt.Sprintf("%s://%s", cfg.Store.Name, cfg.Store.Path)
	syncer.store, err := kv.Open(path)
	if err != nil {
		stop(err)
	}
	defer syncer.store.Close()
	defer s.wg.Done()

	err = s.checkBinlogFormat()
	if err != nil {
		return errors.Trace(err)
	}

	streamer, err := s.syncer.StartSync(s.pos.Pos())
	if err != nil {
		return errors.Trace(err)
	}

	s.start = time.Now()
	s.lastTime = s.start

	go s.sync(s.toDB, s.jobs)// err only one go?

	s.wg.Add(1)
	go s.printStatus()

	pos := s.poz.Pos()

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
