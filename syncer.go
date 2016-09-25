package syncer

import (
	"github.com/juju/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/model"
	pb "github.com/pingcap/tipb/go-binlog"
)

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

func (s *Syncer) s.getHistoryJob(id int64) err {
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
					sql, arg, err := s.db.genInsertSQLs(schema, table, mutation.GetInsertedRows())
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
