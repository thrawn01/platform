package store

import (
	l4g "github.com/alecthomas/log4go"
	rethink "github.com/dancannon/gorethink"
	"github.com/mattermost/platform/model"
	"github.com/mattermost/platform/utils"
)

type RethinkTeamStore struct {
	rethink *rethink.Session
}

func NewRethinkTeamStore(session *rethink.Session) *RethinkTeamStore {
	s := &RethinkTeamStore{session}
	s.CreateTablesIfNotExists()
	s.CreateIndexesIfNotExists()
	return s
}

func (s RethinkTeamStore) UpgradeSchemaIfNeeded() {
}

func (s RethinkTeamStore) CreateTablesIfNotExists() {
	err := rethink.TableCreate("Teams", rethink.TableCreateOpts{PrimaryKey: "Id"}).Exec(s.rethink, execOpts)
	handleCreateError("Teams.CreateTablesIfNotExists()", err)
	err = rethink.TableCreate("TeamMembers").Exec(s.rethink, execOpts)
	handleCreateError("TeamMembers.CreateTablesIfNotExists()", err)
}

func (s RethinkTeamStore) CreateIndexesIfNotExists() {
	// Teams Table
	err := rethink.Table("Teams").IndexCreate("Name").Exec(s.rethink, execOpts)
	handleCreateError("Teams.CreateIndexesIfNotExists().IndexCreate", err)
	err = rethink.Table("Teams").IndexWait("Name").Exec(s.rethink, execOpts)
	handleCreateError("Teams.CreateIndexesIfNotExists().IndexWait", err)

	err = rethink.Table("Teams").IndexCreate("InviteId").Exec(s.rethink, execOpts)
	handleCreateError("Teams.CreateIndexesIfNotExists().IndexCreate", err)
	err = rethink.Table("Teams").IndexWait("InviteId").Exec(s.rethink, execOpts)
	handleCreateError("Teams.CreateIndexesIfNotExists().IndexWait", err)

	// TeamMembers Table
	err = rethink.Table("TeamMembers").IndexCreate("TeamId").Exec(s.rethink, execOpts)
	handleCreateError("Teams.CreateIndexesIfNotExists().IndexCreate", err)
	err = rethink.Table("TeamMembers").IndexWait("TeamId").Exec(s.rethink, execOpts)
	handleCreateError("Teams.CreateIndexesIfNotExists().IndexWait", err)

	err = rethink.Table("TeamMembers").IndexCreate("UserId").Exec(s.rethink, execOpts)
	handleCreateError("Teams.CreateIndexesIfNotExists().IndexCreate", err)
	err = rethink.Table("TeamMembers").IndexWait("UserId").Exec(s.rethink, execOpts)
	handleCreateError("Teams.CreateIndexesIfNotExists().IndexWait", err)

}

func (s RethinkTeamStore) Save(team *model.Team) StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		if len(team.Id) > 0 {
			result.Err = model.NewLocAppError("RethinkTeamStore.Save",
				"store.sql_team.save.existing.app_error", nil, "id="+team.Id)
			storeChannel <- result
			close(storeChannel)
			return
		}

		team.PreSave()

		if result.Err = team.IsValid(*utils.Cfg.TeamSettings.RestrictTeamNames); result.Err != nil {
			l4g.Info("Not Valid")
			storeChannel <- result
			close(storeChannel)
			return
		}

		// TODO: Avoid race condition by using etcd to lock a create request across the cluster
		// TODO: etcd.Lock(/team/team.Name)

		// Ask the DB if this team already exists
		cursor, err := rethink.Table("Teams").GetAllByIndex("Name", team.Name).Run(s.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkTeamStore.Save",
				"store.sql_team.save.get.app_error", nil, "id="+team.Id+", "+err.Error())
			storeChannel <- result
			close(storeChannel)
			return
		}
		if !cursor.IsNil() {
			result.Err = model.NewLocAppError("RethinkTeamStore.Save",
				"store.sql_team.save.get.domain_exists.app_error", nil, "id="+team.Id)
			cursor.Close()
			storeChannel <- result
			close(storeChannel)
			return
		}

		_, err = rethink.Table("Teams").Insert(team).RunWrite(s.rethink, runOpts)
		if err != nil {
			if rethink.IsConflictErr(err) {
				result.Err = model.NewLocAppError("RethinkTeamStore.Save",
					"store.sql_team.save.insert.domain_exists.app_error", nil,
					"id="+team.Id+", "+err.Error())
			} else {
				result.Err = model.NewLocAppError("RethinkTeamStore.Save",
					"store.sql_team.save.insert.app_error", nil, "id="+team.Id+", "+err.Error())
			}
		}
		result.Data = team
		// TODO: etcd.UnLock(/team/team.Name)
		cursor.Close()
		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkTeamStore) Update(team *model.Team) StoreChannel {

	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		team.PreUpdate()

		if result.Err = team.IsValid(*utils.Cfg.TeamSettings.RestrictTeamNames); result.Err != nil {
			storeChannel <- result
			close(storeChannel)
			return
		}
		if changed, err := rethink.Table("Teams").Get(team.Id).
			Update(team).RunWrite(s.rethink, runOpts); err != nil {
			result.Err = model.NewLocAppError("RethinkTeamStore.Update",
				"store.sql_team.update.finding.app_error", nil, "id="+team.Id+", "+err.Error())
		} else if changed.Skipped != 0 {
			// If update was skipped, this means we didn't find the team to update
			result.Err = model.NewLocAppError("RethinkTeamStore.Update",
				"store.sql_team.update.notfound.app_error", nil, "id="+team.Id)
		} else {
			result.Data = team
		}
		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkTeamStore) UpdateDisplayName(name string, teamId string) StoreChannel {

	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		_, err := rethink.Table("Teams").Get(teamId).Update(map[string]interface{}{"DisplayName": name}).
			RunWrite(s.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkTeamStore.UpdateName",
				"store.sql_team.update_display_name.app_error", nil,
				"team_id="+teamId+", "+err.Error())
		}
		result.Data = teamId

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkTeamStore) Get(id string) StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}
		team := &model.Team{}

		cursor, err := rethink.Table("Teams").Get(id).Run(s.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkTeamStore.Get",
				"store.sql_team.get.finding.app_error", nil, "id="+id+", "+err.Error())
		} else if err = cursor.One(team); err != nil {
			result.Err = model.NewLocAppError("RethinkTeamStore.Get",
				"store.sql_team.get.find.app_error", nil, "id="+id)
		} else {
			// Set the inviteId if it is not set
			if len(team.InviteId) == 0 {
				team.InviteId = team.Id
			}
			result.Data = team
		}

		cursor.Close()
		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkTeamStore) GetByInviteId(inviteId string) StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		team := model.Team{}

		cursor, err := rethink.Table("Teams").Filter(rethink.Row.Field("Id").Eq(inviteId).
			Or(rethink.Row.Field("InviteId").Eq(inviteId))).Run(s.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkTeamStore.GetByInviteId",
				"store.sql_team.get_by_invite_id.finding.app_error", nil,
				"inviteId="+inviteId+", "+err.Error())
		} else if err = cursor.One(&team); err != nil {
			result.Err = model.NewLocAppError("RethinkTeamStore.GetByInviteId",
				"store.sql_team.get_by_invite_id.one.find.app_error", nil, "inviteId="+inviteId)
		}

		if len(team.InviteId) == 0 {
			team.InviteId = team.Id
		}

		if len(inviteId) == 0 || team.InviteId != inviteId {
			result.Err = model.NewLocAppError("RethinkTeamStore.GetByInviteId",
				"store.sql_team.get_by_invite_id.find.app_error", nil, "inviteId="+inviteId)
		}

		result.Data = &team

		cursor.Close()
		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkTeamStore) GetByName(name string) StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		team := model.Team{}

		cursor, err := rethink.Table("Teams").Filter(rethink.Row.Field("Name").Eq(name)).Run(s.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkTeamStore.GetByName",
				"store.sql_team.get_by_name.app_error", nil, "name="+name+", "+err.Error())
		} else if err = cursor.One(&team); err != nil {
			result.Err = model.NewLocAppError("RethinkTeamStore.GetByName",
				"store.sql_team.get_by_name.one.app_error", nil, "name="+name+", "+err.Error())
		}

		if len(team.InviteId) == 0 {
			team.InviteId = team.Id
		}

		result.Data = &team

		cursor.Close()
		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkTeamStore) GetAll() StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		var data []*model.Team
		cursor, err := rethink.Table("Teams").Run(s.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkTeamStore.GetAllTeams",
				"store.sql_team.get_all.app_error", nil, err.Error())
		} else if err := cursor.All(&data); err != nil {
			result.Err = model.NewLocAppError("RethinkTeamStore.GetAllTeams",
				"store.sql_team.get_all.all.app_error", nil, err.Error())
		}

		for _, team := range data {
			if len(team.InviteId) == 0 {
				team.InviteId = team.Id
			}
		}

		result.Data = data

		cursor.Close()
		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkTeamStore) GetTeamsByUserId(userId string) StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		var data []*model.Team
		cursor, err := rethink.Table("TeamMembers").
			EqJoin("TeamId", rethink.Table("Teams")).Without(map[string]string{"left": "id"}).
			Zip().Filter(rethink.Row.Field("UserId").Eq(userId)).Run(s.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkTeamStore.GetTeamsByUserId",
				"store.sql_team.get_teams_by_user.app_error", nil, err.Error())
		} else if err := cursor.All(&data); err != nil {
			result.Err = model.NewLocAppError("RethinkTeamStore.GetTeamsByUserId",
				"store.sql_team.get_teams_by_user.cursor.app_error", nil, err.Error())
		}

		for _, team := range data {
			if len(team.InviteId) == 0 {
				team.InviteId = team.Id
			}
		}

		result.Data = data

		cursor.Close()
		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkTeamStore) GetAllTeamListing() StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		var data []*model.Team
		cursor, err := rethink.Table("Teams").
			Filter(rethink.Row.Field("AllowOpenInvite").Eq(true)).Run(s.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkTeamStore.GetAllTeams",
				"store.sql_team.get_all_team_listing.app_error", nil, err.Error())
		} else if err := cursor.All(&data); err != nil {
			result.Err = model.NewLocAppError("RethinkTeamStore.GetAllTeams",
				"store.sql_team.get_all_team_listing.cursor.app_error", nil, err.Error())
		}

		for _, team := range data {
			if len(team.InviteId) == 0 {
				team.InviteId = team.Id
			}
		}

		result.Data = data

		cursor.Close()
		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

// TODO: thrawn01 - This will not notify the user if the team doesn't exist,
// Is this a security feature or should we improve?
func (s RethinkTeamStore) PermanentDelete(teamId string) StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		_, err := rethink.Table("Teams").Get(teamId).Delete().RunWrite(s.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkTeamStore.Delete",
				"store.sql_team.permanent_delete.app_error", nil, "teamId="+teamId+", "+err.Error())
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkTeamStore) AnalyticsTeamCount() StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		count := 0
		cursor, err := rethink.Table("Teams").Field("DeleteAt").Count(0).Run(s.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkTeamStore.AnalyticsTeamCount",
				"store.sql_team.analytics_team_count.app_error", nil, err.Error())
		} else if err := cursor.One(&count); err != nil {
			result.Err = model.NewLocAppError("RethinkTeamStore.AnalyticsTeamCount",
				"store.sql_team.analytics_team_count.cursor.app_error", nil, err.Error())
		} else {
			result.Data = count
		}

		cursor.Close()
		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkTeamStore) SaveMember(member *model.TeamMember) StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		if result.Err = member.IsValid(); result.Err != nil {
			storeChannel <- result
			close(storeChannel)
			return
		}

		// TODO: thrawn01 - This should be it's own method 'IsTeamFull'
		count := 0
		cursor, err := rethink.Table("TeamMembers").Filter(rethink.Row.Field("TeamId").Eq(member.TeamId)).
			Count(0).Run(s.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("SqlUserStore.Save",
				"store.sql_user.save.member_count.app_error", nil,
				"teamId="+member.TeamId+", "+err.Error())
			storeChannel <- result
			close(storeChannel)
			return

		} else if cursor.One(&count); err != nil {
			result.Err = model.NewLocAppError("SqlUserStore.Save",
				"store.sql_user.save.member_count.cursor.app_error", nil,
				"teamId="+member.TeamId+", "+err.Error())
			cursor.Close()
			storeChannel <- result
			close(storeChannel)
			return
		} else if count > utils.Cfg.TeamSettings.MaxUsersPerTeam {
			result.Err = model.NewLocAppError("SqlUserStore.Save",
				"store.sql_user.save.max_accounts.app_error", nil, "teamId="+member.TeamId)
			storeChannel <- result
			close(storeChannel)
			return
		}

		_, err = rethink.Table("TeamMembers").Insert(member).RunWrite(s.rethink, runOpts)
		if err != nil {
			if rethink.IsConflictErr(err) {
				result.Err = model.NewLocAppError("RethinkTeamStore.SaveMember",
					"store.sql_team.save_member.exists.app_error", nil,
					"team_id="+member.TeamId+", user_id="+member.UserId+", "+err.Error())
			} else {
				result.Err = model.NewLocAppError("RethinkTeamStore.SaveMember",
					"store.sql_team.save_member.save.app_error", nil,
					"team_id="+member.TeamId+", user_id="+member.UserId+", "+err.Error())
			}
		} else {
			result.Data = member
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkTeamStore) UpdateMember(member *model.TeamMember) StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		if result.Err = member.IsValid(); result.Err != nil {
			storeChannel <- result
			close(storeChannel)
			return
		}

		_, err := rethink.Table("TeamMembers").Filter(rethink.Row.Field("TeamId").Eq(member.TeamId).
			And(rethink.Row.Field("UserId").Eq(member.UserId))).
			Update(member).RunWrite(s.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkTeamStore.UpdateMember",
				"store.sql_team.save_member.save.app_error", nil, err.Error())
		} else {
			result.Data = member
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkTeamStore) GetMembers(teamId string) StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		var members []*model.TeamMember
		cursor, err := rethink.Table("TeamMembers").Filter(rethink.Row.Field("TeamId").Eq(teamId)).
			Run(s.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkTeamStore.GetMembers",
				"store.sql_team.get_members.app_error", nil, "teamId="+teamId+" "+err.Error())
		} else if err := cursor.All(&members); err != nil {
			result.Err = model.NewLocAppError("RethinkTeamStore.GetMembers",
				"store.sql_team.get_members.cursor.app_error", nil, "teamId="+teamId+" "+err.Error())
		} else {
			result.Data = members
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkTeamStore) GetTeamsForUser(userId string) StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		var members []*model.TeamMember
		cursor, err := rethink.Table("TeamMembers").Filter(rethink.Row.Field("UserId").Eq(userId)).
			Run(s.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkTeamStore.GetMembers",
				"store.sql_team.get_members.app_error", nil, "userId="+userId+" "+err.Error())
		} else if err := cursor.All(&members); err != nil {
			result.Err = model.NewLocAppError("RethinkTeamStore.GetMembers",
				"store.sql_team.get_members.cursor.app_error", nil, "userId="+userId+" "+err.Error())
		} else {
			result.Data = members
		}

		cursor.Close()
		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkTeamStore) RemoveMember(teamId string, userId string) StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		_, err := rethink.Table("TeamMembers").Filter(rethink.Row.Field("TeamId").Eq(teamId).
			And(rethink.Row.Field("UserId").Eq(userId))).
			Delete().RunWrite(s.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("SqlChannelStore.RemoveMember",
				"store.sql_team.remove_member.app_error", nil,
				"team_id="+teamId+", user_id="+userId+", "+err.Error())
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkTeamStore) RemoveAllMembersByTeam(teamId string) StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		_, err := rethink.Table("TeamMembers").Filter(rethink.Row.Field("TeamId").Eq(teamId)).
			Delete().RunWrite(s.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("SqlChannelStore.RemoveMember",
				"store.sql_team.remove_member.app_error", nil, "team_id="+teamId+", "+err.Error())
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkTeamStore) RemoveAllMembersByUser(userId string) StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		_, err := rethink.Table("TeamMembers").Filter(rethink.Row.Field("UserId").Eq(userId)).
			Delete().RunWrite(s.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("SqlChannelStore.RemoveMember",
				"store.sql_team.remove_member.app_error", nil, "user_id="+userId+", "+err.Error())
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}
