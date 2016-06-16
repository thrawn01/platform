package store

import (
	"fmt"
	"strings"

	rethink "github.com/dancannon/gorethink"
	"github.com/mattermost/platform/model"
	"github.com/mattermost/platform/utils"
)

type RethinkUserStore struct {
	rethink *rethink.Session
}

func NewRethinkUserStore(session *rethink.Session) *RethinkUserStore {
	us := &RethinkUserStore{session}
	us.CreateTablesIfNotExists()
	us.CreateIndexesIfNotExists()
	return us
}

func (us RethinkUserStore) UpgradeSchemaIfNeeded() {
	// ADDED for 2.0 REMOVE for 2.4
	//us.CreateColumnIfNotExists("Users", "Locale", "varchar(5)", "character varying(5)", model.DEFAULT_LOCALE)
}

func (us RethinkUserStore) CreateTablesIfNotExists() {
	err := rethink.TableCreate("Users", rethink.TableCreateOpts{PrimaryKey: "Id"}).Exec(us.rethink, execOpts)
	handleCreateError("Users.CreateTablesIfNotExists().", err)
	err = rethink.TableCreate("TeamMembers").Exec(us.rethink, execOpts)
	handleCreateError("TeamMembers.CreateTablesIfNotExists()", err)
}

func (us RethinkUserStore) CreateIndexesIfNotExists() {
	err := rethink.Table("Users").IndexCreate("Email").Exec(us.rethink, execOpts)
	handleCreateError("Users.CreateIndexesIfNotExists().Email.IndexCreate", err)
	err = rethink.Table("Users").IndexWait("Email").Exec(us.rethink, execOpts)
	handleCreateError("Channels.CreateIndexesIfNotExists().Email.IndexWait", err)
}

func (us RethinkUserStore) validEmailAndUsername(user *model.User) *model.AppError {
	// Does a user with this email already exist?
	cursor, err := rethink.Table("Users").Filter(rethink.Row.Field("Email").Eq(user.Email)).Run(us.rethink, runOpts)
	if !cursor.IsNil() {
		cursor.Close()
		return model.NewLocAppError("RethinkUserStore.Save",
			"store.sql_user.save.email_exists.app_error", nil,
			"user_id="+user.Id+", "+err.Error())
	}
	cursor.Close()

	// TODO: thrawn - Shouldn't this be one query instead of two?

	// Does this username already exist?
	cursor, err = rethink.Table("Users").Filter(rethink.Row.Field("Username").Eq(user.Username)).Run(us.rethink, runOpts)
	if !cursor.IsNil() {
		cursor.Close()
		return model.NewLocAppError("RethinkUserStore.Save",
			"store.sql_user.save.username_exists.app_error", nil,
			"user_id="+user.Id+", "+err.Error())
	}
	cursor.Close()
	return nil
}

func (us RethinkUserStore) Save(user *model.User) StoreChannel {

	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		if len(user.Id) > 0 {
			result.Err = model.NewLocAppError("RethinkUserStore.Save", "store.sql_user.save.existing.app_error", nil, "user_id="+user.Id)
			storeChannel <- result
			close(storeChannel)
			return
		}

		user.PreSave()
		if result.Err = user.IsValid(); result.Err != nil {
			storeChannel <- result
			close(storeChannel)
			return
		}

		// Avoid new user race conditions
		err := dLock.Lock("newUser")
		if err != nil {
			result.Err = model.NewLocAppError("RethinkUserStore.Save",
				"store.sql_user.save.lock.app_error", nil, err.Error())
			storeChannel <- result
			close(storeChannel)
			return
		}
		defer func() { dLock.UnLock("newUser") }()

		if err := us.validEmailAndUsername(user); err != nil {
			result.Err = err
			storeChannel <- result
			close(storeChannel)
			return
		}

		// Create the user
		changed, err := rethink.Table("Users").Insert(user).RunWrite(us.rethink, runOpts)
		if err != nil {
			if rethink.IsConflictErr(err) {
				result.Err = model.NewLocAppError("RethinkUserStore.Save",
					"store.sql_user.save.user_id_conflict.app_error", nil,
					"user_id="+user.Id+", "+err.Error())
			} else {
				result.Err = model.NewLocAppError("RethinkUserStore.Save",
					"store.sql_user.save.app_error", nil,
					"user_id="+user.Id+", "+err.Error())
			}
		} else if changed.Inserted != 1 {
			result.Err = model.NewLocAppError("RethinkUserStore.Save",
				"store.sql_user.save.insert.app_error", nil,
				"user_id="+user.Id+", "+changed.FirstError)
		} else {
			result.Data = user
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (us RethinkUserStore) Update(user *model.User, trustedUpdateData bool) StoreChannel {

	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		user.PreUpdate()

		if result.Err = user.IsValid(); result.Err != nil {
			storeChannel <- result
			close(storeChannel)
			return
		}

		checkUser := false
		oldUser := model.User{}
		cursor, err := rethink.Table("Users").Get(user.Id).Run(us.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkUserStore.Update",
				"store.sql_user.update.finding.app_error", nil,
				"user_id="+user.Id+", "+err.Error())
		} else if cursor.IsNil() {
			result.Err = model.NewLocAppError("RethinkUserStore.Update",
				"store.sql_user.update.find.app_error", nil, "user_id="+user.Id)
		} else if err := cursor.One(&oldUser); err != nil {
			result.Err = model.NewLocAppError("RethinkUserStore.Update",
				"store.sql_user.update.finding.cursor.app_error",
				nil, "user_id="+user.Id+", "+err.Error())
		} else {
			user.CreateAt = oldUser.CreateAt
			user.AuthData = oldUser.AuthData
			user.AuthService = oldUser.AuthService
			user.Password = oldUser.Password
			user.LastPasswordUpdate = oldUser.LastPasswordUpdate
			user.LastPictureUpdate = oldUser.LastPictureUpdate
			user.LastActivityAt = oldUser.LastActivityAt
			user.LastPingAt = oldUser.LastPingAt
			user.EmailVerified = oldUser.EmailVerified
			user.FailedAttempts = oldUser.FailedAttempts
			user.MfaSecret = oldUser.MfaSecret
			user.MfaActive = oldUser.MfaActive

			if !trustedUpdateData {
				user.Roles = oldUser.Roles
				user.DeleteAt = oldUser.DeleteAt
			}

			if user.IsOAuthUser() {
				user.Email = oldUser.Email
			} else if user.IsLDAPUser() && !trustedUpdateData {
				if user.Username != oldUser.Username ||
					user.FirstName != oldUser.FirstName ||
					user.LastName != oldUser.LastName ||
					user.Email != oldUser.Email {
					result.Err = model.NewLocAppError("RethinkUserStore.Update", "store.sql_user.update.can_not_change_ldap.app_error", nil, "user_id="+user.Id)
					storeChannel <- result
					close(storeChannel)
					return
				}
			} else if user.Email != oldUser.Email {
				checkUser = true
				user.EmailVerified = false
			}

			if user.Username != oldUser.Username {
				checkUser = true
				user.UpdateMentionKeysFromUsername(oldUser.Username)
			}

			if checkUser {
				// Avoid new user race conditions
				err := dLock.Lock("newUser")
				if err != nil {
					result.Err = model.NewLocAppError("RethinkUserStore.Save",
						"store.sql_user.save.lock.app_error", nil, err.Error())
					storeChannel <- result
					close(storeChannel)
					return

				}
				defer func() { dLock.UnLock("newUser") }()

				if err := us.validEmailAndUsername(user); err != nil {
					result.Err = err
					storeChannel <- result
					close(storeChannel)
					return
				}
			}

			changed, err := rethink.Table("Users").Get(user.Id).Update(user).RunWrite(us.rethink, runOpts)
			if err != nil {
				result.Err = model.NewLocAppError("RethinkUserStore.Update",
					"store.sql_user.update.updating.app_error", nil,
					"user_id="+user.Id+", "+err.Error())
			} else if changed.Skipped != 0 {
				result.Err = model.NewLocAppError("RethinkUserStore.Update",
					"store.sql_user.update.updating.notfound.app_error", nil,
					"user_id="+user.Id+", "+err.Error())
			} else if changed.Updated != 1 {
				result.Err = model.NewLocAppError("RethinkUserStore.Update",
					"store.sql_user.update.app_error", nil,
					fmt.Sprintf("user_id=%v, count=%v", user.Id, changed.Updated))
			} else {
				result.Data = [2]*model.User{user, &oldUser}
			}
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (us RethinkUserStore) UpdateLastPictureUpdate(userId string) StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		curTime := model.GetMillis()

		changed, err := rethink.Table("Users").Get(userId).
			Update(map[string]interface{}{"UpdateAt": curTime, "LastPictureUpdate": curTime}).
			RunWrite(us.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkUserStore.UpdateUpdateAt",
				"store.sql_user.update_last_picture_update.app_error", nil, "user_id="+userId)
		} else if changed.Skipped != 0 {
			result.Err = model.NewLocAppError("RethinkUserStore.UpdateUpdateAt",
				"store.sql_user.update_last_picture_update.notfound.app_error", nil, "user_id="+userId)
		} else {
			result.Data = userId
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (us RethinkUserStore) UpdateLastPingAt(userId string, time int64) StoreChannel {
	storeChannel := make(StoreChannel)

	// UPDATE Users SET LastPingAt = :LastPingAt WHERE Id = :UserId
	go func() {
		result := StoreResult{}

		changed, err := rethink.Table("Users").Get(userId).
			Update(map[string]interface{}{"LastPingAt": time}).
			RunWrite(us.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkUserStore.UpdateLastPingAt",
				"store.sql_user.update_last_ping.app_error", nil, "user_id="+userId)
		} else if changed.Skipped != 0 {
			result.Err = model.NewLocAppError("RethinkUserStore.UpdateLastPingAt",
				"store.sql_user.update_last_ping.notfound.app_error", nil, "user_id="+userId)
		} else {
			result.Data = userId
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (us RethinkUserStore) UpdateLastActivityAt(userId string, time int64) StoreChannel {
	storeChannel := make(StoreChannel)

	// UPDATE Users SET LastActivityAt = :LastActivityAt WHERE Id = :UserId", map[string]interface{}{"LastActivityAt": time, "UserId": userId}); err != nil {
	go func() {
		result := StoreResult{}

		changed, err := rethink.Table("Users").Get(userId).
			Update(map[string]interface{}{"LastActivityAt": time}).
			RunWrite(us.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkUserStore.UpdateLastActivityAt",
				"store.sql_user.update_last_activity.app_error", nil, "user_id="+userId)
		} else if changed.Skipped != 0 {
			result.Err = model.NewLocAppError("RethinkUserStore.UpdateLastActivityAt",
				"store.sql_user.update_last_activity.notfound.app_error", nil, "user_id="+userId)
		} else {
			result.Data = userId
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (us RethinkUserStore) UpdateUserAndSessionActivity(userId string, sessionId string, time int64) StoreChannel {
	storeChannel := make(StoreChannel)

	// UPDATE Users SET LastActivityAt = :UserLastActivityAt WHERE Id = :UserId
	// UPDATE Sessions SET LastActivityAt = :SessionLastActivityAt WHERE Id = :SessionId
	go func() {
		result := StoreResult{}

		changed, err := rethink.Table("Users").Get(userId).
			Update(map[string]interface{}{"LastActivityAt": time}).
			RunWrite(us.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkUserStore.UpdateLastActivityAt",
				"store.sql_user.update_last_activity.app_error", nil,
				"1 user_id="+userId+" session_id="+sessionId+" err="+err.Error())
			storeChannel <- result
			close(storeChannel)
		} else if changed.Skipped != 0 {
			result.Err = model.NewLocAppError("RethinkUserStore.UpdateLastActivityAt",
				"store.sql_user.update_last_activity.notfound.app_error", nil,
				"1 user_id="+userId+" session_id="+sessionId+" err="+changed.FirstError)
			storeChannel <- result
			close(storeChannel)
		}

		changed, err = rethink.Table("Sessions").Get(sessionId).
			Update(map[string]interface{}{"LastActivityAt": time}).
			RunWrite(us.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkUserStore.UpdateLastActivityAt",
				"store.sql_user.update_last_activity.session.app_error", nil,
				"2 user_id="+userId+" session_id="+sessionId+" err="+err.Error())
		} else if changed.Skipped != 0 {
			result.Err = model.NewLocAppError("RethinkUserStore.UpdateLastActivityAt",
				"store.sql_user.update_last_activity.session.notfound.app_error", nil,
				"2 user_id="+userId+" session_id="+sessionId+" err="+changed.FirstError)
		} else {
			result.Data = userId
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (us RethinkUserStore) UpdatePassword(userId, hashedPassword string) StoreChannel {

	storeChannel := make(StoreChannel)

	// UPDATE Users SET Password = :Password, LastPasswordUpdate = :LastPasswordUpdate, UpdateAt = :UpdateAt,
	// AuthData = '', AuthService = '', EmailVerified = true, FailedAttempts = 0 WHERE Id = :UserId
	go func() {
		result := StoreResult{}

		updateAt := model.GetMillis()

		changed, err := rethink.Table("Users").Get(userId).
			Update(map[string]interface{}{
				"Password":           hashedPassword,
				"LastPasswordUpdate": updateAt,
				"UpdateAt":           updateAt,
				"AuthData":           "",
				"AuthService":        "",
				"EmailVerified":      true,
				"FailedAttempts":     0,
			}).RunWrite(us.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkUserStore.UpdatePassword",
				"store.sql_user.update_password.app_error", nil, "id="+userId+", "+err.Error())
		} else if changed.Skipped != 0 {
			result.Err = model.NewLocAppError("RethinkUserStore.UpdatePassword",
				"store.sql_user.update_password.notfound.app_error", nil,
				"id="+userId+", "+changed.FirstError)
		} else {
			result.Data = userId
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (us RethinkUserStore) UpdateFailedPasswordAttempts(userId string, attempts int) StoreChannel {
	storeChannel := make(StoreChannel)

	// UPDATE Users SET FailedAttempts = :FailedAttempts WHERE Id = :UserId
	go func() {
		result := StoreResult{}
		changed, err := rethink.Table("Users").Get(userId).
			Update(map[string]interface{}{
				"FailedAttempts": attempts,
			}).RunWrite(us.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkUserStore.UpdateFailedPasswordAttempts",
				"store.sql_user.update_failed_pwd_attempts.app_error", nil, "user_id="+userId)
		} else if changed.Skipped != 0 {
			result.Err = model.NewLocAppError("RethinkUserStore.UpdateFailedPasswordAttempts",
				"store.sql_user.update_failed_pwd_attempts.notfound.app_error", nil,
				"user_id="+userId+", "+changed.FirstError)
		} else {
			result.Data = userId
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (self RethinkUserStore) UpdateAuthData(userId, service, authData, email string) StoreChannel {

	storeChannel := make(StoreChannel)

	// UPDATE Users SET Password = '', LastPasswordUpdate = :LastPasswordUpdate,
	// UpdateAt = :UpdateAt, FailedAttempts = 0, AuthService = :AuthService,
	// AuthData = :AuthData`
	go func() {
		result := StoreResult{}

		email = strings.ToLower(email)

		updateAt := model.GetMillis()

		update := map[string]interface{}{
			"LastPasswordUpdate": updateAt,
			"UpdateAt":           updateAt,
			"FailedAttempts":     0,
			"AuthService":        service,
			"AuthData":           authData,
		}

		if len(email) != 0 {
			err := dLock.Lock("newUser")
			if err != nil {
				result.Err = model.NewLocAppError("RethinkUserStore.UpdateAuthData",
					"store.sql_user.lock.app_error", nil, err.Error())
				storeChannel <- result
				close(storeChannel)
				return
			}
			defer func() { dLock.UnLock("newUser") }()

			update["Email"] = email
			user, err := self._get(userId)
			if err != nil {
				result.Err = err
				return
			}
			if err := self.validEmailAndUsername(user); err != nil {
				result.Err = err
				storeChannel <- result
				close(storeChannel)
				return
			}
		}

		changed, err := rethink.Table("Users").Get(userId).
			Update(update).RunWrite(self.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkUserStore.UpdateAuthData",
				"store.sql_user.update_auth_data.app_error", nil, "id="+userId+", "+err.Error())
		} else if changed.Skipped != 0 {
			result.Err = model.NewLocAppError("RethinkUserStore.UpdateAuthData",
				"store.sql_user.update_auth_data.notfound.app_error", nil,
				"id="+userId+", "+changed.FirstError)
		} else {
			result.Data = userId
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (us RethinkUserStore) UpdateMfaSecret(userId, secret string) StoreChannel {

	storeChannel := make(StoreChannel)

	// UPDATE Users SET MfaSecret = :Secret, UpdateAt = :UpdateAt WHERE Id = :UserId
	go func() {
		result := StoreResult{}

		updateAt := model.GetMillis()

		changed, err := rethink.Table("Users").Get(userId).
			Update(map[string]interface{}{
				"MfaSecret": secret,
				"UpdateAt":  updateAt,
			}).RunWrite(us.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkUserStore.UpdateMfaSecret",
				"store.sql_user.update_mfa_secret.app_error", nil, "id="+userId+", "+err.Error())
		} else if changed.Skipped != 0 {
			result.Err = model.NewLocAppError("RethinkUserStore.UpdateMfaSecret",
				"store.sql_user.update_mfa_secret.notfound.app_error", nil,
				"id="+userId+", "+changed.FirstError)
		} else {
			result.Data = userId
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (us RethinkUserStore) UpdateMfaActive(userId string, active bool) StoreChannel {

	storeChannel := make(StoreChannel)

	// UPDATE Users SET MfaActive = :Active, UpdateAt = :UpdateAt WHERE Id = :UserId
	go func() {
		result := StoreResult{}

		updateAt := model.GetMillis()

		changed, err := rethink.Table("Users").Get(userId).
			Update(map[string]interface{}{
				"MfaActive": active,
				"UpdateAt":  updateAt,
			}).RunWrite(us.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkUserStore.UpdateMfaActive",
				"store.sql_user.update_mfa_active.app_error", nil, "id="+userId+", "+err.Error())
		} else if changed.Skipped != 0 {
			result.Err = model.NewLocAppError("RethinkUserStore.UpdateMfaActive",
				"store.sql_user.update_mfa_active.notfound.app_error", nil,
				"id="+userId+", "+changed.FirstError)
		} else {
			result.Data = userId
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (self RethinkUserStore) _get(id string) (*model.User, error) {

	user := model.User{}
	cursor, err := rethink.Table("Users").Get(id).Run(self.rethink, runOpts)
	if err != nil {
		return nil, model.NewLocAppError("RethinkUserStore.Get",
			"store.sql_user.get.app_error", nil, "user_id="+id+", "+err.Error())
	} else if cursor.IsNil() {
		return nil, model.NewLocAppError("RethinkUserStore.Get",
			"store.sql_user.missing_account.const", nil, "user_id="+id)
	}
	return &user
}

func (self RethinkUserStore) Get(id string) StoreChannel {

	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{self._get(id)}
		storeChannel <- result
		close(storeChannel)

	}()

	return storeChannel
}

func (us RethinkUserStore) GetAll() StoreChannel {

	storeChannel := make(StoreChannel)

	// SELECT * FROM Users
	go func() {
		result := StoreResult{}

		var data []*model.User
		cursor, err := rethink.Table("Users").Run(us.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkUserStore.GetAll",
				"store.sql_user.get.app_error", nil, err.Error())
		} else if err := cursor.All(&data); err != nil {
			result.Err = model.NewLocAppError("RethinkUserStore.GetAll",
				"store.sql_user.get.cursor.app_error", nil, err.Error())
		} else {
			result.Data = data
		}

		storeChannel <- result
		close(storeChannel)

	}()

	return storeChannel
}

// NOTE: This ignores teams. should it?
func (s RethinkUserStore) GetEtagForDirectProfiles(userId string) StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		// Get the UpdateAt for all users that this userId is in a direct channel with or
		// the user has the 'direct_channel_show' preference set

		// Get all the direct channels for this user

		// SELECT Channels.Id FROM Channels, ChannelMembers WHERE
		//    Channels.Type = 'D' AND Channels.Id = ChannelMembers.ChannelId
		//    AND ChannelMembers.UserId = :UserId)

		// TODO: fix
		/*cursor, err := rethink.Table("Channels").EqJoin("ChannelMembers", rethink.Table("ChannelId")).Without(map[string]string{"right": "id"}).
		Zip().Filter(
			rethink.Row.Field("UserId").Eq(userId).
			And(rethink.Row.Field("DeleteAt").Eq(0)). // Added this, seams logical
			And(rethink.Row.Field("Type").Eq("D"))).
		Run(s.rethink, runOpts)*/

		/*updateAt, err := s.GetReplica().SelectInt(`
					SELECT
					    UpdateAt
					FROM
					    Users
					WHERE
					    Id IN (SELECT DISTINCT
					            UserId
					        FROM
					            ChannelMembers
					        WHERE
					            ChannelMembers.UserId != :UserId
					                AND ChannelMembers.ChannelId IN (SELECT
					                    Channels.Id
					                FROM
					                    Channels,
					                    ChannelMembers
					                WHERE
					                    Channels.Type = 'D'
					                        AND Channels.Id = ChannelMembers.ChannelId
					                        AND ChannelMembers.UserId = :UserId))
					        OR Id IN (SELECT
					            Name
					        FROM
					            Preferences
					        WHERE
					            UserId = :UserId
					                AND Category = 'direct_channel_show')
					ORDER BY UpdateAt DESC
		        `, map[string]interface{}{"UserId": userId})
				if err != nil {
					result.Data = fmt.Sprintf("%v.%v.%v.%v", model.CurrentVersion, model.GetMillis(), utils.Cfg.PrivacySettings.ShowFullName, utils.Cfg.PrivacySettings.ShowEmailAddress)
				} else {
					result.Data = fmt.Sprintf("%v.%v.%v.%v", model.CurrentVersion, updateAt, utils.Cfg.PrivacySettings.ShowFullName, utils.Cfg.PrivacySettings.ShowEmailAddress)
				}*/

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (self RethinkUserStore) GetEtagForAllProfiles() StoreChannel {
	storeChannel := make(StoreChannel)

	// SELECT UpdateAt FROM Users ORDER BY UpdateAt DESC LIMIT 1
	go func() {
		result := StoreResult{}

		var updateAt int64
		cursor, err := rethink.Table("Users").OrderBy(rethink.Desc("UpdateAt")).Limit(1).
			Run(self.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkUserStore.GetEtagForAllProfiles",
				"store.sql_user.get_profiles.app_error", nil, err.Error())
		} else if cursor.IsNil() {
			result.Data = fmt.Sprintf("%v.%v.%v.%v", model.CurrentVersion, model.GetMillis(),
				utils.Cfg.PrivacySettings.ShowFullName, utils.Cfg.PrivacySettings.ShowEmailAddress)
		} else if err := cursor.One(&updateAt); err != nil {
			result.Err = model.NewLocAppError("RethinkUserStore.GetEtagForAllProfiles",
				"store.sql_user.get_profiles.cursor.app_error", nil, err.Error())
		} else {
			result.Data = fmt.Sprintf("%v.%v.%v.%v", model.CurrentVersion, updateAt,
				utils.Cfg.PrivacySettings.ShowFullName, utils.Cfg.PrivacySettings.ShowEmailAddress)
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (us RethinkUserStore) GetAllProfiles() StoreChannel {

	storeChannel := make(StoreChannel)

	// SELECT * FROM Users")
	go func() {
		result := StoreResult{}

		var users []*model.User
		cursor, err := rethink.Table("Users").Run(us.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkUserStore.GetProfiles",
				"store.sql_user.get_profiles.app_error", nil, err.Error())
		} else if err := cursor.All(&users); err != nil {
			result.Err = model.NewLocAppError("RethinkUserStore.GetProfiles",
				"store.sql_user.get_profiles.cursor.app_error", nil, err.Error())
		} else {
			userMap := make(map[string]*model.User)

			for _, u := range users {
				u.Password = ""
				u.AuthData = ""
				userMap[u.Id] = u
			}
			result.Data = userMap
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkUserStore) GetEtagForProfiles(teamId string) StoreChannel {
	storeChannel := make(StoreChannel)

	// SELECT UpdateAt FROM Users, TeamMembers WHERE TeamMembers.TeamId = :TeamId
	// AND Users.Id = TeamMembers.UserId ORDER BY UpdateAt DESC LIMIT 1
	go func() {
		result := StoreResult{}

		var updateAt int64
		cursor, err := rethink.Table("Users").EqJoin("TeamMembers", rethink.Table("UserId")).
			Without(map[string]string{"right": "id"}).Zip().
			Filter(rethink.Row.Field("TeamId").Eq(teamId)).OrderBy(rethink.Row.Field("UpdateAt")).
			Run(s.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkUserStore.GetEtagForProfiles",
				"store.sql_user.get_profiles.app_error", nil, err.Error())
		} else if cursor.IsNil() {
			result.Data = fmt.Sprintf("%v.%v.%v.%v", model.CurrentVersion, model.GetMillis(),
				utils.Cfg.PrivacySettings.ShowFullName, utils.Cfg.PrivacySettings.ShowEmailAddress)
		} else if err := cursor.One(&updateAt); err != nil {
			result.Err = model.NewLocAppError("RethinkUserStore.GetEtagForProfiles",
				"store.sql_user.get_profiles.cursor.app_error", nil, err.Error())
		} else {
			result.Data = fmt.Sprintf("%v.%v.%v.%v", model.CurrentVersion, updateAt,
				utils.Cfg.PrivacySettings.ShowFullName, utils.Cfg.PrivacySettings.ShowEmailAddress)
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (us RethinkUserStore) GetProfiles(teamId string) StoreChannel {

	storeChannel := make(StoreChannel)

	// SELECT Users.* FROM Users, TeamMembers WHERE TeamMembers.TeamId = :TeamId
	// AND Users.Id = TeamMembers.UserId
	go func() {
		result := StoreResult{}

		var users []*model.User
		cursor, err := rethink.Table("Users").EqJoin("TeamMembers", rethink.Table("UserId")).
			Without(map[string]string{"right": "id"}).Zip().
			Filter(rethink.Row.Field("TeamId").Eq(teamId)).
			Run(us.rethink, runOpts)

		if err != nil {
			result.Err = model.NewLocAppError("RethinkUserStore.GetProfiles",
				"store.sql_user.get_profiles.app_error", nil, err.Error())
		} else if err := cursor.All(&users); err != nil {
			result.Err = model.NewLocAppError("RethinkUserStore.GetProfiles",
				"store.sql_user.get_profiles.cursor.app_error", nil, err.Error())
		} else {
			userMap := make(map[string]*model.User)

			for _, u := range users {
				u.Password = ""
				u.AuthData = ""
				userMap[u.Id] = u
			}

			result.Data = userMap
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (us RethinkUserStore) GetDirectProfiles(userId string) StoreChannel {

	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		/*var users []*model.User
				// TODO: fix
				if _, err := us.GetReplica().Select(&users, `
					SELECT
					    Users.*
					FROM
					    Users
					WHERE
					    Id IN (SELECT DISTINCT
					            UserId
					        FROM
					            ChannelMembers
					        WHERE
					            ChannelMembers.UserId != :UserId
					                AND ChannelMembers.ChannelId IN (SELECT
					                    Channels.Id
					                FROM
					                    Channels,
					                    ChannelMembers
					                WHERE
					                    Channels.Type = 'D'
					                        AND Channels.Id = ChannelMembers.ChannelId
					                        AND ChannelMembers.UserId = :UserId))
					        OR Id IN (SELECT
					            Name
					        FROM
					            Preferences
					        WHERE
					            UserId = :UserId
					                AND Category = 'direct_channel_show')
		            `, map[string]interface{}{"UserId": userId}); err != nil {
					result.Err = model.NewLocAppError("RethinkUserStore.GetDirectProfiles", "store.sql_user.get_profiles.app_error", nil, err.Error())
				} else {

					userMap := make(map[string]*model.User)

					for _, u := range users {
						u.Password = ""
						u.AuthData = ""
						userMap[u.Id] = u
					}

					result.Data = userMap
				}*/

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (us RethinkUserStore) GetProfileByIds(userIds []string) StoreChannel {

	storeChannel := make(StoreChannel)

	// SELECT * FROM Users WHERE Users.Id IN ("+idQuery+")"
	go func() {
		result := StoreResult{}

		var users []*model.User
		cursor, err := rethink.Table("Users").Filter(func(channel rethink.Term) {
			rethink.Expr(userIds).Contains(channel.Field("Id"))
		}).Run(us.rethink)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkUserStore.GetProfileByIds",
				"store.sql_user.get_profiles.app_error", nil, err.Error())
		} else if err := cursor.All(&users); err != nil {
			result.Err = model.NewLocAppError("RethinkUserStore.GetProfileByIds",
				"store.sql_user.get_profiles.cursor.app_error", nil, err.Error())
		} else {
			userMap := make(map[string]*model.User)

			for _, u := range users {
				u.Password = ""
				u.AuthData = ""
				userMap[u.Id] = u
			}

			result.Data = userMap
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (us RethinkUserStore) GetSystemAdminProfiles() StoreChannel {

	storeChannel := make(StoreChannel)

	// SELECT * FROM Users WHERE Roles = :Roles", map[string]interface{}{"Roles": "system_admin"}); err != nil {
	go func() {
		result := StoreResult{}

		var users []*model.User

		cursor, err := rethink.Table("Users").Filter(rethink.Row.Field("Roles").Eq("system_admin")).
			Run(us.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkUserStore.GetSystemAdminProfiles",
				"store.sql_user.get_sysadmin_profiles.app_error", nil, err.Error())
		} else if err := cursor.All(&users); err != nil {
			result.Err = model.NewLocAppError("RethinkUserStore.GetSystemAdminProfiles",
				"store.sql_user.get_sysadmin_profiles.cursor.app_error", nil, err.Error())
		} else {

			userMap := make(map[string]*model.User)

			for _, u := range users {
				u.Password = ""
				u.AuthData = ""
				userMap[u.Id] = u
			}

			result.Data = userMap
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (us RethinkUserStore) GetByEmail(email string) StoreChannel {

	storeChannel := make(StoreChannel)

	// SELECT * FROM Users WHERE Email = :Email
	go func() {
		result := StoreResult{}

		email = strings.ToLower(email)

		user := model.User{}

		cursor, err := rethink.Table("Users").Filter(rethink.Row.Field("Email").Eq(email)).
			Run(us.rethink, runOpts)
		if err == nil {
			result.Err = model.NewLocAppError("RethinkUserStore.GetByEmail",
				"store.sql_user.missing_account.const", nil, "email="+email+", "+err.Error())
		} else if err := cursor.One(&user); err != nil {
			result.Err = model.NewLocAppError("RethinkUserStore.GetByEmail",
				"store.sql_user.missing_account.cursor.const", nil, "email="+email+", "+err.Error())
		} else {
			result.Data = &user
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (us RethinkUserStore) GetByAuth(authData string, authService string) StoreChannel {

	storeChannel := make(StoreChannel)

	// SELECT * FROM Users WHERE AuthData = :AuthData AND AuthService = :AuthService
	go func() {
		result := StoreResult{}

		user := model.User{}

		cursor, err := rethink.Table("Users").Filter(
			rethink.Row.Field("AuthData").Eq(authData).
				And(rethink.Row.Field("AuthService").Eq(authService))).
			Run(us.rethink, runOpts)
		if err == nil {
			result.Err = model.NewLocAppError("RethinkUserStore.GetByAuth",
				"store.sql_user.get_by_auth.other.app_error", nil,
				"authData="+authData+", authService="+authService+", "+err.Error())
		} else if cursor.IsNil() {
			result.Err = model.NewLocAppError("RethinkUserStore.GetByAuth", MISSING_AUTH_ACCOUNT_ERROR, nil, "authData="+authData+", authService="+authService+", "+err.Error())
		} else if err := cursor.One(&user); err != nil {
			result.Err = model.NewLocAppError("RethinkUserStore.GetByAuth",
				"store.sql_user.get_by_auth.other.cursor.app_error", nil,
				"authData="+authData+", authService="+authService+", "+err.Error())
		}

		result.Data = &user

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (us RethinkUserStore) GetByUsername(username string) StoreChannel {

	storeChannel := make(StoreChannel)

	// SELECT * FROM Users WHERE Username = :Username
	go func() {
		result := StoreResult{}

		user := model.User{}

		cursor, err := rethink.Table("Users").Filter(
			rethink.Row.Field("Username").Eq(username)).
			Run(us.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkUserStore.GetByUsername",
				"store.sql_user.get_by_username.app_error", nil, err.Error())
		} else if err := cursor.One(&user); err != nil {
			result.Err = model.NewLocAppError("RethinkUserStore.GetByUsername",
				"store.sql_user.cursor.get_by_username.app_error", nil, err.Error())
		} else {
			result.Data = &user
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (us RethinkUserStore) GetForLogin(loginId string, allowSignInWithUsername, allowSignInWithEmail, ldapEnabled bool) StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}
		/*
			params := map[string]interface{}{
				"LoginId":                 loginId,
				"AllowSignInWithUsername": allowSignInWithUsername,
				"AllowSignInWithEmail":    allowSignInWithEmail,
				"LdapEnabled":             ldapEnabled,
			}

			users := []*model.User{}
			if _, err := us.GetReplica().Select(
				&users,
				`SELECT
					*
				FROM
					Users
				WHERE
					(:AllowSignInWithUsername AND Username = :LoginId)
					OR (:AllowSignInWithEmail AND Email = :LoginId)
					OR (:LdapEnabled AND AuthService = '`+model.USER_AUTH_SERVICE_LDAP+`' AND AuthData = :LoginId)`,
				params); err != nil {
				result.Err = model.NewLocAppError("RethinkUserStore.GetForLogin", "store.sql_user.get_for_login.app_error", nil, err.Error())
			} else if len(users) == 1 {
				result.Data = users[0]
			} else if len(users) > 1 {
				result.Err = model.NewLocAppError("RethinkUserStore.GetForLogin", "store.sql_user.get_for_login.multiple_users", nil, "")
			} else {
				result.Err = model.NewLocAppError("RethinkUserStore.GetForLogin", "store.sql_user.get_for_login.app_error", nil, "")
			}
			// TODO: fix
		*/

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (us RethinkUserStore) VerifyEmail(userId string) StoreChannel {
	storeChannel := make(StoreChannel)

	// UPDATE Users SET EmailVerified = true WHERE Id = :UserId
	go func() {
		result := StoreResult{}

		changed, err := rethink.Table("Users").Get(userId).Update(map[string]interface{}{"EmailVerified": true}).
			RunWrite(us.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkUserStore.VerifyEmail",
				"store.sql_user.verify_email.app_error", nil,
				"userId="+userId+", "+err.Error())
		} else if changed.Skipped != 0 {
			result.Err = model.NewLocAppError("RethinkUserStore.VerifyEmail",
				"store.sql_user.verify_email.notfound.app_error", nil,
				"userId="+userId+", "+changed.FirstError)
		}

		result.Data = userId

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (us RethinkUserStore) GetForExport(teamId string) StoreChannel {

	storeChannel := make(StoreChannel)

	// SELECT Users.* FROM Users, TeamMembers WHERE TeamMembers.TeamId = :TeamId AND Users.Id = TeamMembers.UserId
	go func() {
		result := StoreResult{}

		var users []*model.User
		cursor, err := rethink.Table("Users").EqJoin("TeamMembers", rethink.Table("UserId")).
			Without(map[string]string{"right": "id"}).Zip().
			Filter(rethink.Row.Field("TeamId").Eq(teamId)).
			Run(us.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkUserStore.GetForExport",
				"store.sql_user.get_for_export.app_error", nil, err.Error())
		} else if err := cursor.All(&users); err != nil {
			result.Err = model.NewLocAppError("RethinkUserStore.GetForExport",
				"store.sql_user.get_for_export.cursor.app_error", nil, err.Error())
		} else {
			for _, u := range users {
				u.Password = ""
				u.AuthData = ""
			}

			result.Data = users
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (us RethinkUserStore) GetTotalUsersCount() StoreChannel {
	storeChannel := make(StoreChannel)

	// SELECT COUNT(Id) FROM Users
	go func() {
		result := StoreResult{}

		count := 0
		cursor, err := rethink.Table("Users").Count().Run(us.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkUserStore.GetTotalUsersCount",
				"store.sql_user.get_total_users_count.app_error", nil, err.Error())
		} else if err := cursor.One(&count); err != nil {
			result.Err = model.NewLocAppError("RethinkUserStore.GetTotalUsersCount",
				"store.sql_user.get_total_users_count.cursor.app_error", nil, err.Error())
		} else {
			result.Data = count
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (us RethinkUserStore) GetTotalActiveUsersCount() StoreChannel {
	storeChannel := make(StoreChannel)

	// SELECT COUNT(Id) FROM Users WHERE LastActivityAt > :Time
	go func() {
		result := StoreResult{}

		time := model.GetMillis() - (1000 * 60 * 60 * 24)

		count := 0
		cursor, err := rethink.Table("Users").Filter(rethink.Row.Field("LastActivityAt").Gt(time)).
			Count().Run(us.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkUserStore.GetTotalActiveUsersCount",
				"store.sql_user.get_total_active_users_count.app_error", nil, err.Error())
		} else if cursor.One(&count); err != nil {
			result.Err = model.NewLocAppError("RethinkUserStore.GetTotalActiveUsersCount",
				"store.sql_user.get_total_active_users_count.cursor.app_error", nil, err.Error())
		} else {
			result.Data = count
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (us RethinkUserStore) PermanentDelete(userId string) StoreChannel {

	storeChannel := make(StoreChannel)

	// DELETE FROM Users WHERE Id = :UserId
	go func() {
		result := StoreResult{}

		err := rethink.Table("Users").Get(userId).Delete().Exec(us.rethink, execOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkUserStore.PermanentDelete", "store.sql_user.permanent_delete.app_error", nil, "userId="+userId+", "+err.Error())
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (us RethinkUserStore) AnalyticsUniqueUserCount(teamId string) StoreChannel {

	storeChannel := make(StoreChannel)

	// SELECT COUNT(DISTINCT Email) FROM Users, TeamMembers WHERE TeamMembers.TeamId = :TeamId
	// AND Users.Id = TeamMembers.UserId
	go func() {
		result := StoreResult{}

		var term rethink.Term

		if len(teamId) > 0 {
			term = rethink.Table("Users").EqJoin("TeamMembers", rethink.Table("UserId")).
				Filter(rethink.Row.Field("TeamId").Eq(teamId)).
				Distinct(rethink.DistinctOpts{Index: "Email"}).Count()
		} else {
			term = rethink.Table("Users").Count()
		}

		count := 0
		cursor, err := term.Run(us.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkUserStore.AnalyticsUniqueUserCount",
				"store.sql_user.analytics_unique_user_count.app_error", nil, err.Error())
		} else if err := cursor.One(&count); err != nil {
			result.Err = model.NewLocAppError("RethinkUserStore.AnalyticsUniqueUserCount",
				"store.sql_user.analytics_unique_user_count.cursor.app_error", nil, err.Error())
		} else {
			result.Data = count
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (us RethinkUserStore) GetUnreadCount(userId string) StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}
		/*
			TODO: fix
			if count, err := us.GetReplica().SelectInt("SELECT SUM(CASE WHEN c.Type = 'D' THEN (c.TotalMsgCount - cm.MsgCount) ELSE 0 END + cm.MentionCount) FROM Channels c INNER JOIN ChannelMembers cm ON cm.ChannelId = c.Id AND cm.UserId = :UserId", map[string]interface{}{"UserId": userId}); err != nil {
				result.Err = model.NewLocAppError("RethinkUserStore.GetMentionCount", "store.sql_user.get_unread_count.app_error", nil, err.Error())
			} else {
				result.Data = count
			}
		*/
		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}
