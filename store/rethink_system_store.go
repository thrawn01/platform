package store

import (
	rethink "github.com/dancannon/gorethink"
	"github.com/mattermost/platform/model"
)

type RethinkSystemStore struct {
	session *rethink.Session
}

func NewRethinkSystemStore(session *rethink.Session) SystemStore {
	s := &RethinkSystemStore{session}
	s.CreateTablesIfNotExists()
	s.CreateIndexesIfNotExists()
	return s
}

func (self RethinkSystemStore) UpgradeSchemaIfNeeded() {
}

func (self RethinkSystemStore) CreateTablesIfNotExists() {
	err := rethink.TableCreate("System", rethink.TableCreateOpts{PrimaryKey: "Id"}).Exec(self.session, execOpts)
	handleCreateError("System.CreateTablesIfNotExists()", err)
}

func (self RethinkSystemStore) CreateIndexesIfNotExists() {
}

func (self RethinkSystemStore) Save(system *model.System) StoreChannel {

	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		_, err := rethink.Table("Systems").Insert(system).RunWrite(self.session, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkSystemStore.Save",
				"store.rethink_system.save.app_error", nil, "")
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (self RethinkSystemStore) SaveOrUpdate(system *model.System) StoreChannel {

	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		changed, err := rethink.Table("Systems").Filter(rethink.Row.Field("Name").Eq(system.Name)).
			Replace(system).RunWrite(self.session, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkSystemStore.SaveOrUpdate",
				"store.rethink_system.save.get.app_error", nil, "")
		} else if changed.Updated == 0 && changed.Inserted == 0 {
			result.Err = model.NewLocAppError("RethinkSystemStore.SaveOrUpdate",
				"store.rethink_system.save.get.not_inserted_or_updated.app_error", nil,
				changed.FirstError)
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (self RethinkSystemStore) Update(system *model.System) StoreChannel {

	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		_, err := rethink.Table("Systems").Filter(rethink.Row.Field("Name").Eq(system.Name)).
			Update(system).RunWrite(self.session, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkSystemStore.Update",
				"store.rethink_system.update.app_error", nil, "")
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (self RethinkSystemStore) Get() StoreChannel {

	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		var systems []model.System
		props := make(model.StringMap)

		cursor, err := rethink.Table("Systems").Run(self.session, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkSystemStore.Get",
				"store.rethink_system.get.app_error", nil, "")
		} else if err := cursor.All(&systems); err != nil {
			result.Err = model.NewLocAppError("RethinkSystemStore.Get",
				"store.rethink_system.get.cursor.app_error", nil, "")
		} else {
			for _, prop := range systems {
				props[prop.Name] = prop.Value
			}
			result.Data = props
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (self RethinkSystemStore) GetByName(name string) StoreChannel {

	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		var system model.System
		cursor, err := rethink.Table("Systems").Filter(rethink.Row.Field("Name").Eq(name)).
			Run(self.session, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkSystemStore.GetByName",
				"store.rethink_system.get_by_name.app_error", nil, "")
		} else if err := cursor.One(&system); err != nil {
			result.Err = model.NewLocAppError("RethinkSystemStore.GetByName",
				"store.rethink_system.get_by_name.cursor.app_error", nil, "")
		} else {
			result.Data = &system
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}
