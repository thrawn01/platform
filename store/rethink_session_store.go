package store

import (
	l4g "github.com/alecthomas/log4go"
	rethink "github.com/dancannon/gorethink"
	"github.com/mattermost/platform/model"
	"github.com/mattermost/platform/utils"
)

type RethinkSessionStore struct {
	session *rethink.Session
	team    *RethinkTeamStore
}

func NewRethinkSessionStore(session *rethink.Session, teamStore *RethinkTeamStore) SessionStore {
	s := &RethinkSessionStore{session, teamStore}
	s.CreateTablesIfNotExists()
	s.CreateIndexesIfNotExists()
	return s
}

func (me RethinkSessionStore) UpgradeSchemaIfNeeded() {
}

func (s RethinkSessionStore) CreateTablesIfNotExists() {
	err := rethink.TableCreate("Sessions", rethink.TableCreateOpts{PrimaryKey: "Id"}).Exec(s.session, execOpts)
	handleCreateError("Sessions.CreateTablesIfNotExists()", err)
}

func (self RethinkSessionStore) CreateIndexesIfNotExists() {
	err := rethink.Table("Sessions").IndexCreate("UserId").Exec(self.session, execOpts)
	handleCreateError("Sessions.CreateIndexesIfNotExists().UserId.IndexCreate", err)
	err = rethink.Table("Sessions").IndexWait("UserId").Exec(self.session, execOpts)
	handleCreateError("Sessions.CreateIndexesIfNotExists().UserId.IndexWait", err)

	err = rethink.Table("Sessions").IndexCreate("Token").Exec(self.session, execOpts)
	handleCreateError("Sessions.CreateIndexesIfNotExists().Token.IndexCreate", err)
	err = rethink.Table("Sessions").IndexWait("Token").Exec(self.session, execOpts)
	handleCreateError("Sessions.CreateIndexesIfNotExists().Token.IndexWait", err)
}

func (self RethinkSessionStore) Save(session *model.Session) StoreChannel {

	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		if len(session.Id) > 0 {
			result.Err = model.NewLocAppError("RethinkSessionStore.Save",
				"store.rethink_session.save.existing.app_error", nil, "id="+session.Id)
			storeChannel <- result
			close(storeChannel)
			return
		}

		session.PreSave()

		if cur := <-self.CleanUpExpiredSessions(session.UserId); cur.Err != nil {
			l4g.Error(utils.T("store.rethink_session.save.cleanup.error"), cur.Err)
		}

		tcs := self.team.GetTeamsForUser(session.UserId)

		_, err := rethink.Table("Sessions").Insert(session).RunWrite(self.session, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkSessionStore.Save",
				"store.rethink_session.save.app_error", nil, "id="+session.Id+", "+err.Error())
			storeChannel <- result
			close(storeChannel)
			return
		} else {
			result.Data = session
		}

		var rtcs StoreResult
		if rtcs = <-tcs; rtcs.Err != nil {
			result.Err = model.NewLocAppError("RethinkSessionStore.Save",
				"store.rethink_session.save.app_error", nil, "id="+session.Id+", "+rtcs.Err.Error())
			return
		} else {
			session.TeamMembers = rtcs.Data.([]*model.TeamMember)
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (self RethinkSessionStore) Get(sessionIdOrToken string) StoreChannel {

	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		var session model.Session

		cursor, err := rethink.Table("Sessions").Filter(rethink.Row.Field("Token").Eq(sessionIdOrToken).
			Or(rethink.Row.Field("Id").Eq(sessionIdOrToken))).Run(self.session, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkSessionStore.Get",
				"store.rethink_session.get.app_error", nil,
				"sessionIdOrToken="+sessionIdOrToken+", "+err.Error())
		} else if err := cursor.One(&session); err != nil {
			result.Err = model.NewLocAppError("RethinkSessionStore.Get",
				"store.rethink_session.get.cursor.app_error", nil,
				"sessionIdOrToken="+sessionIdOrToken+", "+err.Error())
		} else if cursor.IsNil() {
			result.Err = model.NewLocAppError("RethinkSessionStore.Get",
				"store.rethink_session.get.not_found.app_error", nil,
				"sessionIdOrToken="+sessionIdOrToken)
		} else {
			result.Data = session

			tcs := self.team.GetTeamsForUser(session.UserId)
			var rtcs StoreResult
			if rtcs = <-tcs; rtcs.Err != nil {
				result.Err = model.NewLocAppError("RethinkSessionStore.Get",
					"store.rethink_session.get.app_error", nil,
					"sessionIdOrToken="+sessionIdOrToken+", "+rtcs.Err.Error())
				return
			} else {
				session.TeamMembers = rtcs.Data.([]*model.TeamMember)
			}
		}

		storeChannel <- result
		close(storeChannel)

	}()

	return storeChannel
}

func (self RethinkSessionStore) GetSessions(userId string) StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {

		if cur := <-self.CleanUpExpiredSessions(userId); cur.Err != nil {
			l4g.Error(utils.T("store.rethink_session.get_sessions.error"), cur.Err)
		}

		result := StoreResult{}
		var sessions []*model.Session

		tcs := self.team.GetTeamsForUser(userId)

		cursor, err := rethink.Table("Sessions").Filter(rethink.Row.Field("UserId").Eq(userId)).
			OrderBy(rethink.Desc("LastActivityAt")).Run(self.session, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkSessionStore.GetSessions",
				"store.rethink_session.get_sessions.app_error", nil, err.Error())
		} else if err := cursor.All(&sessions); err != nil {
			result.Err = model.NewLocAppError("RethinkSessionStore.GetSessions",
				"store.rethink_session.get_sessions.cursor.app_error", nil, err.Error())
		} else {
			result.Data = sessions
		}

		var rtcs StoreResult
		if rtcs = <-tcs; rtcs.Err != nil {
			result.Err = model.NewLocAppError("RethinkSessionStore.GetSessions",
				"store.rethink_session.get_sessions.app_error", nil, rtcs.Err.Error())
			return
		} else {
			for _, session := range sessions {
				session.TeamMembers = rtcs.Data.([]*model.TeamMember)
			}
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (self RethinkSessionStore) Remove(sessionIdOrToken string) StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		_, err := rethink.Table("Sessions").Filter(rethink.Row.Field("Token").Eq(sessionIdOrToken).
			Or(rethink.Row.Field("Id").Eq(sessionIdOrToken))).Delete().RunWrite(self.session, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkSessionStore.RemoveSession",
				"store.rethink_session.remove.app_error", nil,
				"id="+sessionIdOrToken+", err="+err.Error())
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (self RethinkSessionStore) RemoveAllSessions() StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		_, err := rethink.Table("Sessions").Delete().RunWrite(self.session, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkSessionStore.RemoveAllSessions",
				"store.rethink_session.remove_all_sessions_for_team.app_error", nil, err.Error())
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (self RethinkSessionStore) PermanentDeleteSessionsByUser(userId string) StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		_, err := rethink.Table("Sessions").Filter(rethink.Row.Field("UserId").Eq(userId)).
			Delete().RunWrite(self.session, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkSessionStore.RemoveAllSessionsForUser",
				"store.rethink_session.permanent_delete_sessions_by_user.app_error", nil,
				"id="+userId+", err="+err.Error())
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (self RethinkSessionStore) CleanUpExpiredSessions(userId string) StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}
		time := model.GetMillis()

		// This request should not return until the sessions are removed
		_, err := rethink.Table("Sessions").Filter(rethink.Row.Field("UserId").Eq(userId).
			And(rethink.Row.Field("ExpiresAt").Gt(time))).Delete().RunWrite(self.session, runOptsHard)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkSessionStore.CleanUpExpiredSessions",
				"store.rethink_session.cleanup_expired_sessions.app_error", nil, err.Error())
		} else {
			result.Data = userId
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (self RethinkSessionStore) UpdateLastActivityAt(sessionId string, time int64) StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		_, err := rethink.Table("Session").Get(sessionId).Update(map[string]interface{}{"LastActivityAt": time}).
			RunWrite(self.session, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkSessionStore.UpdateLastActivityAt",
				"store.rethink_session.update_last_activity.app_error", nil, "sessionId="+sessionId)
		} else {
			result.Data = sessionId
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (self RethinkSessionStore) UpdateRoles(userId, roles string) StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}
		_, err := rethink.Table("Session").Filter(rethink.Row.Field("UserId").Eq(userId)).
			Update(map[string]interface{}{"Roles": roles}).RunWrite(self.session, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkSessionStore.UpdateRoles",
				"store.rethink_session.update_roles.app_error", nil, "userId="+userId)
		} else {
			result.Data = userId
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (self RethinkSessionStore) UpdateDeviceId(id, deviceId string) StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		_, err := rethink.Table("Session").Get(id).
			Update(map[string]interface{}{"DeviceId": deviceId}).RunWrite(self.session, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkSessionStore.UpdateDeviceId",
				"store.rethink_session.update_device_id.app_error", nil, err.Error())
		} else {
			result.Data = deviceId
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (self RethinkSessionStore) AnalyticsSessionCount() StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		time := model.GetMillis()
		var count int64
		cursor, err := rethink.Table("Session").Filter(rethink.Row.Field("ExpiresAt").Gt(time)).
			Count().Run(self.session, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkSessionStore.AnalyticsSessionCount",
				"store.rethink_session.analytics_session_count.app_error", nil, err.Error())
		} else if err := cursor.One(&count); err != nil {

		} else {
			result.Data = count
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}
