package store

import (
	rethink "github.com/dancannon/gorethink"
	"github.com/mattermost/platform/model"
)

type RethinkRecoveryStore struct {
	session *rethink.Session
}

func NewRethinkRecoveryStore(session *rethink.Session) *RethinkRecoveryStore {
	s := &RethinkRecoveryStore{session}
	s.CreateIndexesIfNotExists()
	s.CreateTablesIfNotExists()
	return s
}

func (s RethinkRecoveryStore) UpgradeSchemaIfNeeded() {
}

func (s RethinkRecoveryStore) CreateTablesIfNotExists() {
	err := rethink.TableCreate("Recovery", rethink.TableCreateOpts{PrimaryKey: "Id"}).
		Exec(s.session, execOpts)
	handleCreateError("Recovery.CreateTablesIfNotExists()", err)
}

func (s RethinkRecoveryStore) CreateIndexesIfNotExists() {
	handleCreateError("Recovery.Code.CreateIndexesIfNotExists().IndexCreate",
		rethink.Table("Recovery").IndexCreate("Code").Exec(s.session, execOpts))
	handleCreateError("Recovery.Code.CreateIndexesIfNotExists().IndexWait",
		rethink.Table("Recovery").IndexWait("Code").Exec(s.session, execOpts))
}

func (s RethinkRecoveryStore) SaveOrUpdate(recovery *model.PasswordRecovery) StoreChannel {

	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		recovery.PreSave()
		if result.Err = recovery.IsValid(); result.Err != nil {
			storeChannel <- result
			close(storeChannel)
			return
		}

		cursor, err := rethink.Table("Recovery").
			Filter(rethink.Row.Field("UserId").Eq(recovery.UserId)).
			Run(s.session, runOpts)

		if err != nil {
			result.Err = model.NewLocAppError("RethinkRecoveryStore.SaveOrUpdate",
				"store.sql_recover.update.app_error", nil, err.Error())
		} else if cursor.IsNil() {
			changed, err := rethink.Table("Recovery").Insert(recovery).RunWrite(s.session, runOpts)
			if err != nil {
				result.Err = model.NewLocAppError("RethinkRecoveryStore.SaveOrUpdate",
					"store.sql_recover.save.app_error", nil, "")
			} else if changed.Inserted == 0 {
				result.Err = model.NewLocAppError("RethinkRecoveryStore.SaveOrUpdate",
					"store.sql_recover.save.app_error", nil, changed.FirstError)
			}
		} else {
			changed, err := rethink.Table("Recovery").Get(recovery.UserId).
				Update(recovery).RunWrite(s.session, runOpts)
			if err != nil {
				result.Err = model.NewLocAppError("RethinkRecoveryStore.SaveOrUpdate",
					"store.sql_recover.update.app_error", nil, err.Error())
			} else if changed.Replaced == 0 {
				result.Err = model.NewLocAppError("RethinkRecoveryStore.SaveOrUpdate",
					"store.sql_recover.update.not_found.app_error", nil, "")
			}
		}

		if cursor != nil {
			cursor.Close()
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkRecoveryStore) Delete(userId string) StoreChannel {

	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		_, err := rethink.Table("Recovery").
			Filter(rethink.Row.Field("UserId").Eq(userId)).Delete().
			RunWrite(s.session, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkRecoveryStore.Delete",
				"store.sql_recover.delete.app_error", nil, "")
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkRecoveryStore) Get(userId string) StoreChannel {

	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		recovery := model.PasswordRecovery{}

		cursor, err := rethink.Table("Recovery").
			Filter(rethink.Row.Field("UserId").Eq(userId)).
			Run(s.session, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkRecoveryStore.Get",
				"store.sql_recover.get.app_error", nil, "")
		} else if err := cursor.One(&recovery); err != nil {
			result.Err = model.NewLocAppError("RethinkRecoveryStore.Get",
				"store.sql_recover.get.cursor.app_error", nil, "")
		}

		result.Data = &recovery

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkRecoveryStore) GetByCode(code string) StoreChannel {

	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		recovery := model.PasswordRecovery{}

		cursor, err := rethink.Table("Recovery").
			Filter(rethink.Row.Field("Code").Eq(code)).
			Run(s.session, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkRecoveryStore.GetByCode",
				"store.sql_recover.get_by_code.app_error", nil, "")
		} else if err := cursor.One(&recovery); err != nil {
			result.Err = model.NewLocAppError("RethinkRecoveryStore.GetByCode",
				"store.sql_recover.get_by_code.cursor.app_error", nil, "")
		}

		result.Data = &recovery

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}
