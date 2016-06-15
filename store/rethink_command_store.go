package store

import (
	rethink "github.com/dancannon/gorethink"
	"github.com/mattermost/platform/model"
)

type RethinkCommandStore struct {
	rethink *rethink.Session
}

func NewRethinkCommandStore(session *rethink.Session) CommandStore {
	s := &RethinkCommandStore{session}
	s.CreateIndexesIfNotExists()
	s.CreateTablesIfNotExists()
	return s
}

func (s RethinkCommandStore) UpgradeSchemaIfNeeded() {
}

func (s RethinkCommandStore) CreateTablesIfNotExists() {
	err := rethink.TableCreate("Commands", rethink.TableCreateOpts{PrimaryKey: "Id"}).Exec(s.rethink, execOpts)
	handleCreateError("Commands.CreateTablesIfNotExists()", err)
}

func (s RethinkCommandStore) CreateIndexesIfNotExists() {
	err := rethink.Table("Commands").IndexCreate("TeamId").Exec(s.rethink, execOpts)
	handleCreateError("Commands.CreateIndexesIfNotExists().IndexCreate", err)
	err = rethink.Table("Commands").IndexWait("TeamId").Exec(s.rethink, execOpts)
	handleCreateError("Commands.CreateIndexesIfNotExists().IndexWait", err)
}

func (s RethinkCommandStore) Save(command *model.Command) StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		if len(command.Id) > 0 {
			result.Err = model.NewLocAppError("RethinkCommandStore.Save",
				"store.rethink_command.save.saving_overwrite.app_error", nil, "id="+command.Id)
			storeChannel <- result
			close(storeChannel)
			return
		}

		command.PreSave()
		if result.Err = command.IsValid(); result.Err != nil {
			storeChannel <- result
			close(storeChannel)
			return
		}

		_, err := rethink.Table("Commands").Insert(command).RunWrite(s.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkCommandStore.Save",
				"store.rethink_command.save.saving.app_error", nil, "id="+command.Id+", "+err.Error())
		} else {
			result.Data = command
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkCommandStore) Get(id string) StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		var command model.Command

		cursor, err := rethink.Table("Commands").Get(id).Run(s.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkCommandStore.Get",
				"store.rethink_command.get.app_error", nil, "id="+id+", err="+err.Error())
		} else if err = cursor.One(command); err != nil {
			result.Err = model.NewLocAppError("RethinkCommandStore.Get",
				"store.rethink_command.cursor.get.app_error", nil, "id="+id+", err="+err.Error())
		} else if command.DeleteAt != 0 {
			result.Err = model.NewLocAppError("RethinkCommandStore.Get",
				"store.rethink_command.deleted.get.app_error", nil, "id="+id)
		}

		if cursor != nil {
			cursor.Close()
		}

		result.Data = &command

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkCommandStore) GetByTeam(teamId string) StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		var commands []*model.Command

		cursor, err := rethink.Table("Commands").Filter(rethink.Row.Field("TeamId").Eq(teamId).
			And(rethink.Row.Field("DeleteAt").Eq(0))).Run(s.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkCommandStore.GetByTeam",
				"store.rethink_command.get_team.app_error", nil,
				"teamId="+teamId+", err="+err.Error())
		} else if cursor.All(&commands); err != nil {
			result.Err = model.NewLocAppError("RethinkCommandStore.GetByTeam",
				"store.rethink_command.get_team.cursor.app_error", nil, "teamId="+teamId)
		}

		if cursor != nil {
			cursor.Close()
		}

		result.Data = commands

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkCommandStore) Delete(commandId string, time int64) StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		_, err := rethink.Table("Commands").Get(commandId).
			Update(map[string]interface{}{"DeleteAt": time, "UpdateAt": time}).
			RunWrite(s.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkCommandStore.Delete",
				"store.rethink_command.update.delete.app_error", nil,
				"id="+commandId+", err="+err.Error())
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkCommandStore) PermanentDeleteByUser(userId string) StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		_, err := rethink.Table("Commands").Filter(rethink.Row.Field("CreatorId").Eq(userId)).
			Delete().RunWrite(s.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkCommandStore.DeleteByUser",
				"store.rethink_command.save.delete_perm.app_error", nil,
				"id="+userId+", err="+err.Error())
		}
		// TODO: thrawn - Should error if rows not found?

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkCommandStore) Update(cmd *model.Command) StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		cmd.UpdateAt = model.GetMillis()

		changed, err := rethink.Table("Commands").Get(cmd.Id).Update(cmd).
			RunWrite(s.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkCommandStore.Update",
				"store.rethink_command.save.update.app_error", nil, "id="+cmd.Id+", "+err.Error())
		} else if changed.Skipped != 0 {
			result.Err = model.NewLocAppError("RethinkCommandStore.Update",
				"store.rethink_command.save.not_found.app_error", nil, "id="+cmd.Id)
		} else {
			result.Data = cmd
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkCommandStore) AnalyticsCommandCount(teamId string) StoreChannel {
	storeChannel := make(StoreChannel)

	//SELECT COUNT(*) FROM Commands WHERE DeleteAt = 0 AND TeamId = :TeamId"
	go func() {
		result := StoreResult{}

		filter := rethink.Row.Field("DeleteAt").Eq(0)
		if len(teamId) > 0 {
			filter = filter.And(rethink.Row.Field("TeamId").Eq(teamId))
		}

		var count int64
		cursor, err := rethink.Table("Commands").Filter(filter).Count().Run(s.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkCommandStore.AnalyticsCommandCount",
				"store.rethink_command.analytics_command_count.app_error", nil, err.Error())
		} else if err := cursor.One(&count); err != nil {
			result.Err = model.NewLocAppError("RethinkCommandStore.AnalyticsCommandCount",
				"store.rethink_command.analytics_command_count.cursor.app_error", nil, err.Error())
		} else {
			result.Data = count
		}

		if cursor != nil {
			cursor.Close()
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}
