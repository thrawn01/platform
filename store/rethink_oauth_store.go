package store

import (
	rethink "github.com/dancannon/gorethink"
	"github.com/mattermost/platform/model"
)

type RethinkOAuthStore struct {
	session *rethink.Session
}

func NewRethinkOAuthStore(session *rethink.Session) OAuthStore {
	s := &RethinkOAuthStore{session}
	s.CreateTablesIfNotExists()
	s.CreateIndexesIfNotExists()
	return s
}

func (self RethinkOAuthStore) UpgradeSchemaIfNeeded() {
}

func (s RethinkOAuthStore) CreateTablesIfNotExists() {
	err := rethink.TableCreate("OAuthApps", rethink.TableCreateOpts{PrimaryKey: "Id"}).Exec(s.session, execOpts)
	handleCreateError("OAuthApps.CreateTablesIfNotExists()", err)
	err = rethink.TableCreate("OAuthAccessData").Exec(s.session, execOpts)
	handleCreateError("OAuthAccessData.CreateTablesIfNotExists()", err)
	err = rethink.TableCreate("OAuthAuthData").Exec(s.session, execOpts)
	handleCreateError("OAuthAuthData.CreateTablesIfNotExists()", err)
}

func (self RethinkOAuthStore) CreateIndexesIfNotExists() {
	err := rethink.Table("OAuthApps").IndexCreate("CreatorId").Exec(self.session, execOpts)
	handleCreateError("OAuthApps.CreateIndexesIfNotExists().CreatorId.IndexCreate", err)
	err = rethink.Table("OAuthApps").IndexWait("CreatorId").Exec(self.session, execOpts)
	handleCreateError("OAuthApps.CreateIndexesIfNotExists().CreatorId.IndexWait", err)

	err = rethink.Table("OAuthAccessData").IndexCreate("AuthCode").Exec(self.session, execOpts)
	handleCreateError("OAuthAccessData.CreateIndexesIfNotExists().AuthCode.IndexCreate", err)
	err = rethink.Table("OAuthAccessData").IndexWait("AuthCode").Exec(self.session, execOpts)
	handleCreateError("OAuthAccessData.CreateIndexesIfNotExists().AuthCode.IndexWait", err)

	err = rethink.Table("OAuthAccessData").IndexCreate("Code").Exec(self.session, execOpts)
	handleCreateError("OAuthAccessData.CreateIndexesIfNotExists().Code.IndexCreate", err)
	err = rethink.Table("OAuthAccessData").IndexWait("Code").Exec(self.session, execOpts)
	handleCreateError("OAuthAccessData.CreateIndexesIfNotExists().Code.IndexWait", err)
}

func (self RethinkOAuthStore) SaveApp(app *model.OAuthApp) StoreChannel {

	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		if len(app.Id) > 0 {
			result.Err = model.NewLocAppError("RethinkOAuthStore.SaveApp",
				"store.rethink_oauth.save_app.existing.app_error", nil, "app_id="+app.Id)
			storeChannel <- result
			close(storeChannel)
			return
		}

		app.PreSave()
		if result.Err = app.IsValid(); result.Err != nil {
			storeChannel <- result
			close(storeChannel)
			return
		}

		changed, err := rethink.Table("OAuthApps").Insert(app).RunWrite(self.session, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkOAuthStore.SaveApp",
				"store.rethink_oauth.save_app.save.app_error", nil,
				"app_id="+app.Id+", "+err.Error())
		} else if changed.Inserted != 1 {
			result.Err = model.NewLocAppError("RethinkOAuthStore.SaveApp",
				"store.rethink_oauth.save_app.save.not_inserted.app_error", nil,
				"app_id="+app.Id+", "+changed.FirstError)
		} else {
			result.Data = app
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (self RethinkOAuthStore) UpdateApp(app *model.OAuthApp) StoreChannel {

	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		app.PreUpdate()

		if result.Err = app.IsValid(); result.Err != nil {
			storeChannel <- result
			close(storeChannel)
			return
		}

		var oldApp model.OAuthApp
		cursor, err := rethink.Table("OAuthApps").Get(app.Id).Run(self.session, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkOAuthStore.UpdateApp",
				"store.rethink_oauth.update_app.finding.app_error", nil,
				"app_id="+app.Id+", "+err.Error())
		} else if cursor.IsNil() {
			result.Err = model.NewLocAppError("RethinkOAuthStore.UpdateApp",
				"store.rethink_oauth.update_app.find.app_error", nil, "app_id="+app.Id)
		} else if err := cursor.One(&oldApp); err != nil {
			result.Err = model.NewLocAppError("RethinkOAuthStore.UpdateApp",
				"store.rethink_oauth.update_app.finding.cursor.app_error", nil,
				"app_id="+app.Id+", "+err.Error())
		} else {
			app.CreateAt = oldApp.CreateAt
			app.ClientSecret = oldApp.ClientSecret
			app.CreatorId = oldApp.CreatorId

			changed, err := rethink.Table("OAuthApps").Get(app.Id).Update(app).
				RunWrite(self.session, runOpts)
			if err != nil {
				result.Err = model.NewLocAppError("RethinkOAuthStore.UpdateApp",
					"store.rethink_oauth.update_app.updating.app_error", nil,
					"app_id="+app.Id+", "+err.Error())
			} else if changed.Replaced != 1 {
				result.Err = model.NewLocAppError("RethinkOAuthStore.UpdateApp",
					"store.rethink_oauth.update_app.not_updated.app_error", nil,
					"app_id="+app.Id)
			} else {
				result.Data = [2]*model.OAuthApp{app, &oldApp}
			}
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (self RethinkOAuthStore) GetApp(id string) StoreChannel {

	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		var app model.OAuthApp

		cursor, err := rethink.Table("OAuthApps").Get(app.Id).Run(self.session, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkOAuthStore.GetApp",
				"store.rethink_oauth.get_app.finding.app_error", nil, "app_id="+id+", "+err.Error())
		} else if cursor.IsNil() {
			result.Err = model.NewLocAppError("RethinkOAuthStore.GetApp",
				"store.rethink_oauth.get_app.find.app_error", nil, "app_id="+id)
		} else if err := cursor.One(&app); err != nil {
			result.Err = model.NewLocAppError("RethinkOAuthStore.GetApp",
				"store.rethink_oauth.get_app.finding.cursor.app_error", nil, "app_id="+id+", "+err.Error())
		} else {
			result.Data = app
		}

		storeChannel <- result
		close(storeChannel)

	}()

	return storeChannel
}

func (self RethinkOAuthStore) GetAppByUser(userId string) StoreChannel {

	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		var apps []*model.OAuthApp

		cursor, err := rethink.Table("OAuthApps").Filter(rethink.Row.Field("CreatorId").Eq(userId)).
			Run(self.session, runOpts)

		if err != nil {
			result.Err = model.NewLocAppError("RethinkOAuthStore.GetAppByUser",
				"store.rethink_oauth.get_app_by_user.find.app_error", nil,
				"user_id="+userId+", "+err.Error())
		} else if err := cursor.All(&apps); err != nil {
			result.Err = model.NewLocAppError("RethinkOAuthStore.GetAppByUser",
				"store.rethink_oauth.get_app_by_user.find.app_error", nil,
				"user_id="+userId+", "+err.Error())
		} else {
			result.Data = apps
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (self RethinkOAuthStore) SaveAccessData(accessData *model.AccessData) StoreChannel {

	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		if result.Err = accessData.IsValid(); result.Err != nil {
			storeChannel <- result
			close(storeChannel)
			return
		}

		changed, err := rethink.Table("OAuthAccessData").Insert(accessData).RunWrite(self.session, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkOAuthStore.SaveAccessData",
				"store.rethink_oauth.save_access_data.app_error", nil, err.Error())
		} else if changed.Inserted != 1 {
			result.Err = model.NewLocAppError("RethinkOAuthStore.SaveAccessData",
				"store.rethink_oauth.save_access_data.not_inserted.app_error", nil, changed.FirstError)
		} else {
			result.Data = accessData
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (self RethinkOAuthStore) GetAccessData(token string) StoreChannel {

	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		accessData := model.AccessData{}
		cursor, err := rethink.Table("OAuthAccessData").
			Filter(rethink.Row.Field("Token").Eq(token)).Run(self.session, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkOAuthStore.GetAccessData",
				"store.rethink_oauth.get_access_data.app_error", nil, err.Error())
		} else if err := cursor.One(&accessData); err != nil {
			result.Err = model.NewLocAppError("RethinkOAuthStore.GetAccessData",
				"store.rethink_oauth.get_access_data.cursor.app_error", nil, err.Error())
		} else {
			result.Data = &accessData
		}

		storeChannel <- result
		close(storeChannel)

	}()

	return storeChannel
}

func (self RethinkOAuthStore) GetAccessDataByAuthCode(authCode string) StoreChannel {

	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		accessData := model.AccessData{}

		cursor, err := rethink.Table("OAuthAccessData").
			Filter(rethink.Row.Field("AuthCode").Eq(authCode)).Run(self.session, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkOAuthStore.GetAccessDataByAuthCode",
				"store.rethink_oauth.get_access_data_by_code.app_error", nil, err.Error())
		} else if cursor.IsNil() {
			result.Data = nil
		} else if err := cursor.One(&accessData); err != nil {
			result.Err = model.NewLocAppError("RethinkOAuthStore.GetAccessDataByAuthCode",
				"store.rethink_oauth.get_access_data_by_code.cursor.app_error", nil, err.Error())
		} else {
			result.Data = &accessData
		}

		storeChannel <- result
		close(storeChannel)

	}()

	return storeChannel
}

func (self RethinkOAuthStore) RemoveAccessData(token string) StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		_, err := rethink.Table("OAuthAccessData").
			Filter(rethink.Row.Field("Token").Eq(token)).Delete().
			RunWrite(self.session, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkOAuthStore.RemoveAccessData",
				"store.rethink_oauth.remove_access_data.app_error", nil, "err="+err.Error())
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (self RethinkOAuthStore) SaveAuthData(authData *model.AuthData) StoreChannel {

	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		authData.PreSave()
		if result.Err = authData.IsValid(); result.Err != nil {
			storeChannel <- result
			close(storeChannel)
			return
		}
		_, err := rethink.Table("OAuthAuthData").Insert(authData).RunWrite(self.session, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkOAuthStore.SaveAuthData",
				"store.rethink_oauth.save_auth_data.app_error", nil, err.Error())
		} else {
			result.Data = authData
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (self RethinkOAuthStore) GetAuthData(code string) StoreChannel {

	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}
		var authData model.AuthData
		cursor, err := rethink.Table("OAuthAuthData").
			Filter(rethink.Row.Field("Code").Eq(code)).Run(self.session, runOpts)

		if err != nil {
			result.Err = model.NewLocAppError("RethinkOAuthStore.GetAuthData",
				"store.rethink_oauth.get_auth_data.finding.app_error", nil, err.Error())
		} else if cursor.IsNil() {
			result.Err = model.NewLocAppError("RethinkOAuthStore.GetAuthData",
				"store.rethink_oauth.get_auth_data.find.app_error", nil, "")
		} else if err := cursor.One(&authData); err != nil {
			result.Err = model.NewLocAppError("RethinkOAuthStore.GetAuthData",
				"store.rethink_oauth.get_auth_data.finding.app_error", nil, err.Error())
		} else {
			result.Data = authData
		}

		storeChannel <- result
		close(storeChannel)

	}()

	return storeChannel
}

func (self RethinkOAuthStore) RemoveAuthData(code string) StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		_, err := rethink.Table("OAuthAuthData").
			Filter(rethink.Row.Field("Code").Eq(code)).Delete().RunWrite(self.session, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkOAuthStore.RemoveAuthData",
				"store.rethink_oauth.remove_auth_data.app_error", nil, "err="+err.Error())
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (self RethinkOAuthStore) PermanentDeleteAuthDataByUser(userId string) StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		_, err := rethink.Table("OAuthAuthData").
			Filter(rethink.Row.Field("UserId").Eq(userId)).Delete().RunWrite(self.session, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkOAuthStore.RemoveAuthDataByUserId", "store.rethink_oauth.permanent_delete_auth_data_by_user.app_error", nil, "err="+err.Error())
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}
