package store

import (
	l4g "github.com/alecthomas/log4go"
	rethink "github.com/dancannon/gorethink"
	"github.com/mattermost/platform/model"
	"github.com/mattermost/platform/utils"
)

type RethinkPreferenceStore struct {
	session *rethink.Session
}

func NewRethinkPreferenceStore(session *rethink.Session) PreferenceStore {
	s := &RethinkPreferenceStore{session}
	s.CreateIndexesIfNotExists()
	s.CreateTablesIfNotExists()
	return s
}

func (self RethinkPreferenceStore) UpgradeSchemaIfNeeded() {
}

func (self RethinkPreferenceStore) CreateTablesIfNotExists() {
	err := rethink.TableCreate("Preferences", rethink.TableCreateOpts{PrimaryKey: "UserId"}).Exec(self.session, execOpts)
	handleCreateError("Preferences.CreateTablesIfNotExists()", err)
}

func (self RethinkPreferenceStore) CreateIndexesIfNotExists() {
	err := rethink.Table("Preferences").IndexCreate("UserId").Exec(self.session, execOpts)
	handleCreateError("Preferences.CreateIndexesIfNotExists().UserId.IndexCreate", err)
	err = rethink.Table("Preferences").IndexWait("UserId").Exec(self.session, execOpts)
	handleCreateError("Preferences.CreateIndexesIfNotExists().UserId.IndexWait", err)

	/*err = rethink.Table("Preferences").IndexCreate("Category").Exec(self.rethink, execOpts)
	handleCreateError("Preferences.CreateIndexesIfNotExists().Category.IndexCreate", err)
	err = rethink.Table("Preferences").IndexWait("Category").Exec(self.rethink, execOpts)
	handleCreateError("Preferences.CreateIndexesIfNotExists().Category.IndexWait", err)

	err = rethink.Table("Preferences").IndexCreate("Name").Exec(self.rethink, execOpts)
	handleCreateError("Preferences.CreateIndexesIfNotExists().Name.IndexCreate", err)
	err = rethink.Table("Preferences").IndexWait("Name").Exec(self.rethink, execOpts)
	handleCreateError("Preferences.CreateIndexesIfNotExists().Name.IndexWait", err)*/
}

func (self RethinkPreferenceStore) DeleteUnusedFeatures() {
	l4g.Debug(utils.T("store.rethink_preference.delete_unused_features.debug"))

	rethink.Table("Preferences").Filter(rethink.Row.Field("Preferences").
		Contains(func(row rethink.Term) rethink.Term {
			return row.Field("Value").Eq(false).
				And(rethink.Row.Field("Category").Eq(model.PREFERENCE_CATEGORY_ADVANCED_SETTINGS)).
				And(rethink.Row.Field("Name").Match(FEATURE_TOGGLE_PREFIX))
		})).Delete().Exec(self.session, execOpts)
}

func (self RethinkPreferenceStore) Save(preferences *model.Preferences) StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		rethinkPref := model.RethinkPreferencesFromPreferences(preferences)
		changed, err := rethink.Table("Preferences").Replace(rethinkPref).RunWrite(self.session, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkPreferenceStore.save",
				"store.rethink_preference.save.app_error", nil, err.Error())
		} else if changed.Replaced == 0 && changed.Inserted == 0 {
			result.Err = model.NewLocAppError("RethinkPreferenceStore.save",
				"store.rethink_preference.save.update_insert.app_error", nil, changed.FirstError)
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (self RethinkPreferenceStore) Get(userId string, category string, name string) StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		var preference model.RethinkPreferences

		cursor, err := rethink.Table("Preferences").Get(userId).Run(self.session, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkPreferenceStore.Get",
				"store.rethink_preference.get.app_error", nil, err.Error())
		} else if err := cursor.One(&preference); err != nil {
			result.Err = model.NewLocAppError("RethinkPreferenceStore.Get",
				"store.rethink_preference.get.cursor.app_error", nil, err.Error())
		} else {
			// TODO: thrawn - should we return an error if we couldn't find this preference?
			result.Data, _ = preference.GetPreference(name, category)
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (self RethinkPreferenceStore) GetCategory(userId string, category string) StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		var preference model.RethinkPreferences

		cursor, err := rethink.Table("Preferences").Get(userId).Run(self.session, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkPreferenceStore.GetCategory",
				"store.rethink_preference.get.app_error", nil, err.Error())
		} else if err := cursor.One(&preference); err != nil {
			result.Err = model.NewLocAppError("RethinkPreferenceStore.GetCategory",
				"store.rethink_preference.get.cursor.app_error", nil, err.Error())
		} else {
			// TODO: thrawn - should we return an error if we couldn't any categories?
			result.Data = preference.GetCategories(category)
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (self RethinkPreferenceStore) GetAll(userId string) StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		var preference model.RethinkPreferences
		cursor, err := rethink.Table("Preferences").Get(userId).Run(self.session, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkPreferenceStore.GetAll",
				"store.rethink_preference.get.app_error", nil, err.Error())
		} else if err := cursor.One(&preference); err != nil {
			result.Err = model.NewLocAppError("RethinkPreferenceStore.GetAll",
				"store.rethink_preference.get.cursor.app_error", nil, err.Error())
		} else {
			// TODO: thrawn - should we return an error if we couldn't any categories?
			result.Data = preference.Preferences
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (self RethinkPreferenceStore) PermanentDeleteByUser(userId string) StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		_, err := rethink.Table("Preferences").Get(userId).Delete().RunWrite(self.session, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkPreferenceStore.Delete",
				"store.rethink_preference.permanent_delete_by_user.app_error", nil, err.Error())
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (self RethinkPreferenceStore) IsFeatureEnabled(feature, userId string) StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		var preference model.RethinkPreferences
		cursor, err := rethink.Table("Preferences").Get(userId).Run(self.session, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkPreferenceStore.IsFeatureEnabled",
				"store.rethink_preference.is_feature_enabled.app_error", nil, err.Error())
		} else if err := cursor.One(&preference); err != nil {
			result.Err = model.NewLocAppError("RethinkPreferenceStore.IsFeatureEnabled",
				"store.rethink_preference.is_feature_enabled.cursor.app_error", nil, err.Error())
		} else {
			_, ok := preference.GetPreference(model.PREFERENCE_CATEGORY_ADVANCED_SETTINGS,
				FEATURE_TOGGLE_PREFIX+feature)
			result.Data = ok
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}
