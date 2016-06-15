package store

import (
	rethink "github.com/dancannon/gorethink"
	"github.com/mattermost/platform/model"
)

type RethinkLicenseStore struct {
	rethink *rethink.Session
}

func NewRethinkLicenseStore(sqlStore *RethinkStore) LicenseStore {
	s := &RethinkLicenseStore{sqlStore}
	s.CreateTablesIfNotExists()
	s.CreateIndexesIfNotExists()
	return s
}

func (self RethinkLicenseStore) UpgradeSchemaIfNeeded() {
}

func (self RethinkLicenseStore) CreateTablesIfNotExists() {
	err := rethink.TableCreate("Licenses", rethink.TableCreateOpts{PrimaryKey: "Id"}).Exec(self.rethink, execOpts)
	handleCreateError("Licenses.CreateTablesIfNotExists()", err)
}

func (self RethinkLicenseStore) CreateIndexesIfNotExists() {
}

func (self RethinkLicenseStore) Save(license *model.LicenseRecord) StoreChannel {

	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		license.PreSave()
		if result.Err = license.IsValid(); result.Err != nil {
			storeChannel <- result
			close(storeChannel)
			return
		}

		// Only insert if not exists
		cursor, err := rethink.Table("Licenses").Get(license.Id).Run(self.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkLicenseStore.Save",
				"store.sql_license.save.app_error", nil, "license_id="+license.Id+", "+err.Error())
		} else if cursor.IsNil() {
			changed, err := rethink.Table("Licenses").Insert(license).RunWrite(self.rethink, runOpts)
			if err != nil {
				result.Err = model.NewLocAppError("RethinkLicenseStore.Save",
					"store.sql_license.save.insert.app_error", nil,
					"license_id="+license.Id+", "+err.Error())
			} else if changed.Inserted != 1 {
				result.Err = model.NewLocAppError("RethinkLicenseStore.Save",
					"store.sql_license.save.not_inserted.app_error", nil,
					"license_id="+license.Id+", "+err.Error())
			} else {
				result.Data = license
			}
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (self RethinkLicenseStore) Get(id string) StoreChannel {

	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		var license model.LicenseRecord
		cursor, err := rethink.Table("Licenses").Get(id).Run(self.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkLicenseStore.Get",
				"store.sql_license.get.app_error", nil, "license_id="+id+", "+err.Error())
		} else if cursor.IsNil() {
			result.Err = model.NewLocAppError("RethinkLicenseStore.Get",
				"store.sql_license.get.missing.app_error", nil, "license_id="+id)
		} else if err := cursor.One(&license); err != nil {
			result.Err = model.NewLocAppError("RethinkLicenseStore.Get",
				"store.sql_license.get.cursor.app_error", nil, "license_id="+id+", "+err.Error())
		} else {
			result.Data = license
		}

		storeChannel <- result
		close(storeChannel)

	}()

	return storeChannel
}
