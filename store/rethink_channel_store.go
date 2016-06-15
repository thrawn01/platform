package store

import (
	"strings"

	rethink "github.com/dancannon/gorethink"
	"github.com/dancannon/gorethink/encoding"
	"github.com/mattermost/platform/model"
	"github.com/mattermost/platform/utils"
)

type RethinkChannelStore struct {
	rethink *rethink.Session
}

func NewRethinkChannelStore(session *rethink.Session) *RethinkChannelStore {
	s := &RethinkChannelStore{session}
	s.CreateTablesIfNotExists()
	s.CreateIndexesIfNotExists()
	return s
}

func (s RethinkChannelStore) UpgradeSchemaIfNeeded() {
}

func (s RethinkChannelStore) CreateTablesIfNotExists() {
	err := rethink.TableCreate("Channels", rethink.TableCreateOpts{PrimaryKey: "Id"}).Exec(s.rethink, execOpts)
	handleCreateError("Channels.CreateTablesIfNotExists().", err)
	err = rethink.TableCreate("ChannelMembers", rethink.TableCreateOpts{PrimaryKey: "Id"}).Exec(s.rethink, execOpts)
	handleCreateError("ChannelMembers.CreateTablesIfNotExists()", err)
}

func (s RethinkChannelStore) CreateIndexesIfNotExists() {
	err := rethink.Table("Channels").IndexCreate("Name").Exec(s.rethink, execOpts)
	handleCreateError("Channels.CreateIndexesIfNotExists().IndexCreate", err)
	err = rethink.Table("Channels").IndexWait("Name").Exec(s.rethink, execOpts)
	handleCreateError("Channels.CreateIndexesIfNotExists().IndexWait", err)

	/*s.CreateIndexIfNotExists("idx_channels_team_id", "Channels", "TeamId")
	s.CreateIndexIfNotExists("idx_channels_name", "Channels", "Name")

	s.CreateIndexIfNotExists("idx_channelmembers_channel_id", "ChannelMembers", "ChannelId")
	s.CreateIndexIfNotExists("idx_channelmembers_user_id", "ChannelMembers", "UserId")*/
}

func (s RethinkChannelStore) Save(channel *model.Channel) StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		var result StoreResult
		if channel.Type == model.CHANNEL_DIRECT {
			result.Err = model.NewLocAppError("RethinkChannelStore.Save",
				"store.rethink_channel.save.direct_channel.app_error", nil, "")
		} else {
			if len(channel.Id) > 0 {
				result.Err = model.NewLocAppError("RethinkChannelStore.Save",
					"store.rethink_channel.save.existing.app_error", nil, "id="+channel.Id)
			} else {
				// Lock by team, so we don't create duplicate or to many channels
				err := dLock.Lock(channel.TeamId)
				if err != nil {
					result.Err = model.NewLocAppError("RethinkChannelStore.Save",
						"store.rethink_channel.save.lock.app_error", nil, err.Error())
				} else {
					defer func() {
						dLock.UnLock(channel.Name)
					}()
					channel.PreSave()
					result = s.saveChannelT(channel)
				}
			}
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkChannelStore) SaveDirectChannel(channel *model.Channel, member1 *model.ChannelMember, member2 *model.ChannelMember) StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		var result StoreResult

		if channel.Type != model.CHANNEL_DIRECT {
			result.Err = model.NewLocAppError("RethinkChannelStore.SaveDirectChannel",
				"store.rethink_channel.save_direct_channel.not_direct.app_error", nil, "")
			storeChannel <- result
			close(storeChannel)
			return
		}

		channel.TeamId = ""
		channel.PreSave()

		member1.ChannelId = channel.Id
		member2.ChannelId = channel.Id

		member1.PreSave()
		if result.Err = member1.IsValid(); result.Err != nil {
			storeChannel <- result
			close(storeChannel)
			return
		}

		member2.PreSave()
		if result.Err = member2.IsValid(); result.Err != nil {
			storeChannel <- result
			close(storeChannel)
			return
		}

		newTransaction := model.Transaction{
			State: "new",
			Type:  "DirectChannel",
			Model: model.DirectChannelTransaction{
				Channel: channel,
				Members: []*model.ChannelMember{
					member1,
					member2,
				},
			},
		}

		watch, err := rethink.Table("Transactions").Changes().Run(s.rethink)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkChannelStore.SaveDirectChannel",
				"store.rethink_channel.save_direct_channel.watch.app_error", nil, "")
		}

		newTransaction.PreSave()
		_, err = rethink.Table("Transactions").Insert(newTransaction).RunWrite(s.rethink)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkChannelStore.SaveDirectChannel",
				"store.rethink_channel.save_direct_channel.new_transaction.app_error", nil, "")
		}

		// Wait for the key to be deleted, this indicates the transaction is complete
		var change map[string]*model.Transaction
		for watch.Next(&change) {
			new := change["new_val"]
			if new != nil {
				if new.Id == newTransaction.Id && new.ErrorStr != "" {
					result.Err = model.NewLocAppError("RethinkChannelStore.SaveDirectChannel",
						new.ErrorStr, nil, "")
					break
				}
			}
			// if new_val is null, this indicates a record deletion
			if new == nil {
				// If our newly created transaction was deleted, we know the transaction completed
				if change["old_val"].Id == newTransaction.Id {
					break
				}
			}
		}
		watch.Close()
		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkChannelStore) HandleDirectChannelTransaction(change *model.Transaction, update func(*model.Transaction) error) error {
	var result error
	for {
		if change.State != "new" {
			if err := update(change); err != nil {
				return err
			}
			if change.State == "done" {
				return result
			}
		}

		if change.State == "new" {
			var transaction model.DirectChannelTransaction
			err := encoding.Decode(&transaction, change.Model)
			if err != nil {
				return err
			}

			result := s.saveChannelT(transaction.Channel)
			if result.Err != nil {
				if !strings.Contains("exists", result.Err.Message) {
					change.ErrorStr = result.Err.Error()
					change.State = "done"
					continue
				}
			}
			change.State = "member1"
			continue
		}
		if change.State == "member1" {
			var transaction model.DirectChannelTransaction
			err := encoding.Decode(&transaction, change.Model)
			if err != nil {
				return err
			}

			result := s.saveMemberT(transaction.Members[0])
			if result.Err != nil {
				return result.Err
			}
			change.State = "member2"
			continue
		}

		if change.State == "member2" {
			var transaction model.DirectChannelTransaction
			err := encoding.Decode(&transaction, change.Model)
			if err != nil {
				return err
			}
			result := s.saveMemberT(transaction.Members[1])
			if result.Err != nil {
				return result.Err
			}
			change.State = "done"
			continue
		}
	}
	return nil
}

func (s RethinkChannelStore) doesChannelExist(channel *model.Channel) StoreResult {
	result := StoreResult{}

	cursor, err := rethink.Table("Channels").Filter(
		rethink.Row.Field("Name").Eq(channel.Name).
			And(rethink.Row.Field("TeamId").Eq(channel.TeamId))).
		Run(s.rethink, runOpts)
	if err != nil {
		result.Err = model.NewLocAppError("RethinkChannelStore.Save",
			"store.rethink_channel.save.does_channel.exist.app_error", nil,
			"id="+channel.Id+", "+err.Error())
		return result
	} else if !cursor.IsNil() {
		// A Channel with that name already exists
		dupChannel := model.Channel{}
		if err := cursor.One(&dupChannel); err != nil {
			result.Err = model.NewLocAppError("RethinkChannelStore.Save",
				"store.rethink_channel.save_channel.exists.cursor.app_error", nil,
				"id="+channel.Id+", "+err.Error())
			return result
		} else {
			// If it was a previously deleted channel, return the deleted channel
			if dupChannel.DeleteAt > 0 { // TODO: thrawn - Should not matter if it was deleted or not
				result.Err = model.NewLocAppError("RethinkChannelStore.Save",
					"store.rethink_channel.save_channel.exists.previously.app_error", nil,
					"id="+channel.Id)
				result.Data = &dupChannel
			} else {
				result.Err = model.NewLocAppError("RethinkChannelStore.Save",
					"store.rethink_channel.save_channel.exists.app_error", nil,
					"id="+channel.Id)
				result.Data = &dupChannel
			}
		}
	}
	return result
}

func (s RethinkChannelStore) saveChannelT(channel *model.Channel) StoreResult {
	result := StoreResult{}

	if result.Err = channel.IsValid(); result.Err != nil {
		return result
	}

	var count int64
	if channel.Type != model.CHANNEL_DIRECT {
		// Are we past our 1,000 channel limit?
		cursor, err := rethink.Table("Channels").Filter(
			rethink.Row.Field("TeamId").Eq(channel.TeamId).
				And(rethink.Row.Field("Type").Eq("O").Or(rethink.Row.Field("Type").Eq("P"))).
				And(rethink.Row.Field("DeleteAt").Eq(0))).
			Count().Run(s.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkChannelStore.Save",
				"store.rethink_channel.save_channel.current_count.app_error", nil,
				"teamId="+channel.TeamId+", "+err.Error())
			return result
		} else if err := cursor.One(&count); err != nil {
			result.Err = model.NewLocAppError("RethinkChannelStore.Save",
				"store.rethink_channel.save_channel.current_count.cursor.app_error", nil,
				"teamId="+channel.TeamId+", "+err.Error())
			return result
		} else if count > 1000 {
			result.Err = model.NewLocAppError("RethinkChannelStore.Save",
				"store.rethink_channel.save_channel.limit.app_error", nil, "teamId="+channel.TeamId)
			return result
		}
	}

	exists := s.doesChannelExist(channel)
	if exists.Err != nil {
		return exists
	}

	// No Channel with this name exists
	_, err := rethink.Table("Channels").Get(channel.Id).Replace(channel).RunWrite(s.rethink, runOpts)
	if err != nil {
		result.Err = model.NewLocAppError("RethinkChannelStore.Save",
			"store.rethink_channel.save_channel.insert.app_error", nil, "id="+channel.Id+", "+err.Error())
	} else {
		result.Data = channel
	}

	return result
}

func (s RethinkChannelStore) Update(channel *model.Channel) StoreChannel {

	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		channel.PreUpdate()

		if result.Err = channel.IsValid(); result.Err != nil {
			storeChannel <- result
			close(storeChannel)
			return
		}

		// Are we asking to change the name?
		if len(channel.Name) > 0 {
			err := dLock.Lock(channel.TeamId)
			if err != nil {
				result.Err = model.NewLocAppError("RethinkChannelStore.Update",
					"store.rethink_channel.lock.app_error", nil, err.Error())
				goto UpdateDone
			}
			defer func() { dLock.UnLock(channel.TeamId) }()

			exists := s.doesChannelExist(channel)
			if exists.Err != nil {
				// If this update results in same name on different channels
				if exists.Data.(*model.Channel).Id != channel.Id {
					result.Err = exists.Err
					goto UpdateDone
				}
			}
		}

		if changed, err := rethink.Table("Channels").Get(channel.Id).Update(channel).
			RunWrite(s.rethink, runOpts); err != nil {
			result.Err = model.NewLocAppError("RethinkChannelStore.Update",
				"store.rethink_channel.update.updating.app_error", nil,
				"id="+channel.Id+", "+err.Error())
		} else if changed.Skipped != 0 {
			result.Err = model.NewLocAppError("RethinkChannelStore.Update",
				"store.rethink_channel.update.notfound.app_error", nil,
				"id="+channel.Id+", "+changed.FirstError)
		} else {
			result.Data = channel
		}
	UpdateDone:
		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkChannelStore) extraUpdated(channel *model.Channel) StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		channel.ExtraUpdated()

		changed, err := rethink.Table("Channels").Get(channel.Id).
			Update(map[string]interface{}{"ExtraUpdateAt": channel.ExtraUpdateAt}).
			RunWrite(s.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkChannelStore.extraUpdated",
				"store.rethink_channel.extra_updated.app_error", nil,
				"id="+channel.Id+", "+err.Error())
		} else if changed.Skipped != 0 {
			result.Err = model.NewLocAppError("RethinkChannelStore.extraUpdated",
				"store.rethink_channel.extra_updated.notfound.app_error", nil,
				"id="+channel.Id+", "+changed.FirstError)
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkChannelStore) Get(id string) StoreChannel {
	return s.get(id, false)
}

func (s RethinkChannelStore) GetFromMaster(id string) StoreChannel {
	return s.get(id, true)
}

func (s RethinkChannelStore) get(id string, master bool) StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}
		channel := model.Channel{}

		if cursor, err := rethink.Table("Channels").Get(id).Run(s.rethink, runOpts); err != nil {
			result.Err = model.NewLocAppError("RethinkChannelStore.Get",
				"store.rethink_channel.get.find.app_error", nil, "id="+id+", "+err.Error())
		} else if cursor.IsNil() {
			result.Err = model.NewLocAppError("RethinkChannelStore.Get",
				"store.rethink_channel.get.notfound.app_error", nil, "id="+id)
		} else if err := cursor.One(&channel); err != nil {
			result.Err = model.NewLocAppError("RethinkChannelStore.Get",
				"store.rethink_channel.get.cursor.app_error", nil, "id="+id+", "+err.Error())
		} else {
			result.Data = &channel
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkChannelStore) Delete(channelId string, time int64) StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		if changed, err := rethink.Table("Channels").Get(channelId).
			Update(map[string]interface{}{"DeleteAt": time, "UpdateAt": time}).
			RunWrite(s.rethink, runOpts); err != nil {
			result.Err = model.NewLocAppError("RethinkChannelStore.Delete",
				"store.rethink_channel.delete.channel.app_error", nil,
				"id="+channelId+", err="+err.Error())
		} else if changed.Skipped != 0 {
			result.Err = model.NewLocAppError("RethinkChannelStore.Delete",
				"store.rethink_channel.delete.channel.notfound.app_error", nil,
				"id="+channelId+", err="+changed.FirstError)
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkChannelStore) PermanentDeleteByTeam(teamId string) StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		if changed, err := rethink.Table("Channels").Filter(rethink.Row.Field("TeamId").Eq(teamId)).Delete().
			RunWrite(s.rethink, runOpts); err != nil {
			result.Err = model.NewLocAppError("RethinkChannelStore.PermanentDeleteByTeam",
				"store.rethink_channel.permanent_delete_by_team.app_error", nil,
				"teamId="+teamId+", "+err.Error())
		} else if changed.Skipped != 0 {
			result.Err = model.NewLocAppError("RethinkChannelStore.PermanentDeleteByTeam",
				"store.rethink_channel.permanent_delete_by_team.notfound.app_error", nil,
				"teamId="+teamId+", "+changed.FirstError)
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkChannelStore) GetChannels(teamId string, userId string) StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		// "SELECT * FROM Channels, ChannelMembers WHERE Id = ChannelId AND UserId = :UserId
		// AND DeleteAt = 0 AND (TeamId = :TeamId OR TeamId = '') ORDER BY DisplayName"
		channelList, err := s.getChannelsForUser(teamId, userId)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkChannelStore.GetChannels",
				"store.rethink_channel.get_channels.get.app_error", nil,
				"teamId="+teamId+", userId="+userId+", err="+err.Error())
		} else {
			if len(channelList.Channels) == 0 {
				result.Err = model.NewLocAppError("RethinkChannelStore.GetChannels",
					"store.rethink_channel.get_channels.not_found.app_error", nil,
					"teamId="+teamId+", userId="+userId)
			} else {
				result.Data = channelList
			}
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkChannelStore) getChannelsForUser(teamId string, userId string) (*model.ChannelList, error) {
	// "SELECT * FROM Channels, ChannelMembers WHERE Id = ChannelId AND UserId = :UserId AND DeleteAt = 0
	// AND (TeamId = :TeamId OR TeamId = '') ORDER BY DisplayName"
	cursor, err := rethink.Table("ChannelMembers").EqJoin("ChannelId", rethink.Table("Channels")).
		Without(map[string]string{"left": "Id"}).
		Zip().Filter(
		rethink.Row.Field("UserId").Eq(userId).And(rethink.Row.Field("DeleteAt").Eq(0)).
			And(rethink.Row.Field("TeamId").Eq(teamId).Or(rethink.Row.Field("TeamId").Eq("")))).
		OrderBy(rethink.Row.Field("DisplayName")).
		Run(s.rethink, runOpts)
	if err != nil {
		return nil, err
	}

	channels := &model.ChannelList{make([]*model.Channel, 0), make(map[string]*model.ChannelMember)}

	var row map[string]interface{}
	for cursor.Next(&row) {
		// Extract the Channel
		channel := model.Channel{}
		err := encoding.Decode(&channel, row)
		if err != nil {
			cursor.Close()
			return nil, err
		}

		// Extract the Member
		member := model.ChannelMember{}
		err = encoding.Decode(&member, row)
		if err != nil {
			cursor.Close()
			return nil, err
		}

		channels.Channels = append(channels.Channels, &channel)
		channels.Members[channel.Id] = &member
	}
	cursor.Close()

	return channels, nil
}

// Returns a list of all channels in the team, the user is currently not already in
func (s RethinkChannelStore) GetMoreChannels(teamId string, userId string) StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		// Get a list of Channels available to the user
		userChannels, err := s.getChannelsForUser(teamId, userId)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkChannelStore.GetMoreChannels",
				"store.rethink_channel.get_channels_for_user.app_error", nil,
				"teamId="+teamId+", userId="+userId+", err="+err.Error())
			storeChannel <- result
			close(storeChannel)
			return
		}
		channelIds := make([]string, len(userChannels.Channels))
		for i := 0; i < len(userChannels.Channels); i++ {
			channelIds[i] = userChannels.Channels[i].Id
		}
		// `SELECT * FROM Channels WHERE TeamId = :TeamId1 AND Type IN ('O') AND DeleteAt = 0 AND Id
		// NOT IN (SELECT Channels.Id FROM Channels, ChannelMembers WHERE Id = ChannelId
		// AND TeamId = :TeamId2 AND UserId = :UserId AND DeleteAt = 0) ORDER BY DisplayName`,
		var data []*model.Channel
		cursor, err := rethink.Table("Channels").Filter(func(channel rethink.Term) rethink.Term {
			return rethink.Expr(channelIds).Contains(channel.Field("Id")).Not().
				And(channel.Field("TeamId").Eq(teamId)).
				And(channel.Field("DeleteAt").Eq(0)).
				And(channel.Field("Type").Eq("O"))
		}).OrderBy(rethink.Row.Field("DisplayName")).Run(s.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkChannelStore.GetMoreChannels",
				"store.rethink_channel.get_more_channels.get.app_error", nil,
				"teamId="+teamId+", userId="+userId+", err="+err.Error())
		} else if err = cursor.All(&data); err != nil {
			result.Err = model.NewLocAppError("RethinkChannelStore.GetMoreChannels",
				"store.rethink_channel.get_more_channels.all.app_error", nil,
				"teamId="+teamId+", userId="+userId+", err="+err.Error())
		} else {
			result.Data = &model.ChannelList{data, make(map[string]*model.ChannelMember)}
		}

		// TODO: thrawn - Do this for the rest of the code base
		if cursor != nil {
			cursor.Close()
		}
		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkChannelStore) GetChannelCounts(teamId string, userId string) StoreChannel {
	storeChannel := make(StoreChannel)
	// SELECT Id, TotalMsgCount, UpdateAt FROM Channels WHERE Id IN (SELECT ChannelId FROM ChannelMembers
	// WHERE UserId = :UserId) AND (TeamId = :TeamId OR TeamId = '') AND DeleteAt = 0 ORDER BY DisplayName

	go func() {
		result := StoreResult{}

		userChannels, err := s.getChannelsForUser(teamId, userId)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkChannelStore.GetChannelCounts",
				"store.rethink_channel.get_channels_for_user.app_error", nil,
				"teamId="+teamId+", userId="+userId+", err="+err.Error())
			storeChannel <- result
			close(storeChannel)
			return
		}

		counts := &model.ChannelCounts{Counts: make(map[string]int64), UpdateTimes: make(map[string]int64)}
		for i := 0; i < len(userChannels.Channels); i++ {
			counts.Counts[userChannels.Channels[i].Id] = userChannels.Channels[i].TotalMsgCount
			counts.UpdateTimes[userChannels.Channels[i].Id] = userChannels.Channels[i].UpdateAt
		}

		result.Data = counts
		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkChannelStore) GetByName(teamId string, name string) StoreChannel {
	storeChannel := make(StoreChannel)

	// SELECT * FROM Channels WHERE (TeamId = :TeamId OR TeamId = '') AND Name = :Name AND DeleteAt = 0"

	go func() {
		result := StoreResult{}

		channel := model.Channel{}

		cursor, err := rethink.Table("Channels").Filter(
			rethink.Row.Field("Name").Eq(name).And(rethink.Row.Field("DeleteAt").Eq(0)).
				And(rethink.Row.Field("TeamId").Eq(teamId).Or(rethink.Row.Field("TeamId").Eq("")))).
			Run(s.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkChannelStore.GetByName",
				"store.rethink_channel.get_by_name.existing.app_error", nil,
				"teamId="+teamId+", "+"name="+name+", "+err.Error())
		} else if err = cursor.One(&channel); err != nil {
			if err == rethink.ErrEmptyResult {
				result.Err = model.NewLocAppError("RethinkChannelStore.GetByName",
					"store.rethink_channel.get_by_name.missing.app_error", nil,
					"teamId="+teamId+", "+"name="+name+", "+err.Error())
			} else {
				result.Err = model.NewLocAppError("RethinkChannelStore.GetByName",
					"store.rethink_channel.get_by_name.one.app_error", nil,
					"teamId="+teamId+", "+"name="+name+", "+err.Error())
			}
		} else {
			result.Data = &channel
		}

		cursor.Close()
		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkChannelStore) getChannel(id string) (*model.Channel, error) {
	cursor, err := rethink.Table("Channels").Get(id).Run(s.rethink, runOpts)
	if err != nil {
		return nil, err
	}
	channel := model.Channel{}
	if err = cursor.One(&channel); err != nil {
		return nil, err
	}
	return &channel, nil
}

func (s RethinkChannelStore) SaveMember(member *model.ChannelMember) StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		var result StoreResult
		member.PreSave()
		result = s.saveMemberT(member)
		if result.Err == nil {
			channel, err := s.getChannel(member.ChannelId)
			if err != nil {
				result.Err = model.NewLocAppError("RethinkChannelStore.SaveMember",
					"store.rethink_channel.save_member.getChannel.app_error", nil,
					"channel_id="+member.ChannelId+", user_id="+
						member.UserId+", "+err.Error())
			} else {
				// If successful record members have changed in channel
				if mu := <-s.extraUpdated(channel); mu.Err != nil {
					result.Err = mu.Err
				}
			}
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkChannelStore) saveMemberT(member *model.ChannelMember) StoreResult {
	result := StoreResult{}

	if result.Err = member.IsValid(); result.Err != nil {
		return result
	}
	changed, err := rethink.Table("ChannelMembers").Get(member.Id).
		Replace(member).RunWrite(s.rethink, runOpts)
	if err != nil {
		result.Err = model.NewLocAppError("RethinkChannelStore.SaveMember",
			"store.rethink_channel.save_member.save.app_error", nil,
			"channel_id="+member.ChannelId+", user_id="+member.UserId+", "+err.Error())
	} else if changed.Inserted == 0 && changed.Replaced == 0 {
		result.Err = model.NewLocAppError("RethinkChannelStore.SaveMember",
			"store.rethink_channel.save_member.save.not_inserted.app_error", nil,
			"channel_id="+member.ChannelId+", user_id="+member.UserId+", "+changed.FirstError)
	}
	result.Data = member
	return result
}

func (s RethinkChannelStore) UpdateMember(member *model.ChannelMember) StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		member.PreUpdate()

		if result.Err = member.IsValid(); result.Err != nil {
			storeChannel <- result
			close(storeChannel)
			return
		}

		// TODO: thrawn - Can this now be updated via the 'Id' field?
		changed, err := rethink.Table("ChannelMembers").Filter(
			rethink.Row.Field("ChannelId").Eq(member.ChannelId).
				And(rethink.Row.Field("UserId").Eq(member.UserId))).
			Update(member).RunWrite(s.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkChannelStore.UpdateMember",
				"store.rethink_channel.update_member.app_error", nil,
				"channel_id="+member.ChannelId+", "+"user_id="+member.UserId+", "+err.Error())
		} else if changed.Skipped != 0 {
			result.Err = model.NewLocAppError("RethinkChannelStore.UpdateMember",
				"store.rethink_channel.update_member.notfound.app_error", nil,
				"channel_id="+member.ChannelId+", "+"user_id="+member.UserId+", "+changed.FirstError)
		} else {
			result.Data = member
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkChannelStore) GetMembers(channelId string) StoreChannel {
	storeChannel := make(StoreChannel)

	// SELECT * FROM ChannelMembers WHERE ChannelId = :ChannelId
	go func() {
		result := StoreResult{}

		var members []model.ChannelMember
		cursor, err := rethink.Table("ChannelMembers").Filter(
			rethink.Row.Field("ChannelId").Eq(channelId)).
			Run(s.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkChannelStore.GetMembers",
				"store.rethink_channel.get_members.app_error", nil,
				"channel_id="+channelId+err.Error())
		} else if err = cursor.All(&members); err != nil {
			result.Err = model.NewLocAppError("RethinkChannelStore.GetMembers",
				"store.rethink_channel.get_members.cursor.app_error", nil,
				"channel_id="+channelId+err.Error())
		} else {
			result.Data = members
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkChannelStore) GetMember(channelId string, userId string) StoreChannel {
	storeChannel := make(StoreChannel)

	// SELECT * FROM ChannelMembers WHERE ChannelId = :ChannelId AND UserId = :UserId"
	go func() {
		result := StoreResult{}

		var member model.ChannelMember
		cursor, err := rethink.Table("ChannelMembers").Filter(
			rethink.Row.Field("ChannelId").Eq(member.ChannelId).
				And(rethink.Row.Field("UserId").Eq(member.UserId))).
			Run(s.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkChannelStore.GetMember",
				"store.rethink_channel.get_member.app_error", nil,
				"channel_id="+channelId+"user_id="+userId+","+err.Error())
		} else if err = cursor.One(&member); err != nil {
			if err == rethink.ErrEmptyResult {
				result.Err = model.NewLocAppError("RethinkChannelStore.GetMember",
					"store.rethink_channel.get_member.missing.app_error", nil,
					"channel_id="+channelId+"user_id="+userId+","+err.Error())
			} else {
				result.Err = model.NewLocAppError("RethinkChannelStore.GetMember",
					"store.rethink_channel.get_member.cursor.app_error", nil,
					"channel_id="+channelId+"user_id="+userId+","+err.Error())
			}
		} else {
			result.Data = member
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkChannelStore) GetMemberCount(channelId string) StoreChannel {
	storeChannel := make(StoreChannel)

	// SELECT count(*) FROM ChannelMembers, Users WHERE ChannelMembers.UserId = Users.Id
	// AND ChannelMembers.ChannelId = :ChannelId AND Users.DeleteAt = 0
	go func() {
		result := StoreResult{}

		var count int64
		cursor, err := rethink.Table("ChannelMembers").Filter(
			rethink.Row.Field("ChannelId").Eq(channelId)).
			Count().Run(s.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkChannelStore.GetMemberCount",
				"store.rethink_channel.get_member_count.app_error", nil,
				"channel_id="+channelId+", "+err.Error())
		} else if cursor.One(&count); err != nil {
			result.Err = model.NewLocAppError("RethinkChannelStore.GetMemberCount",
				"store.rethink_channel.get_member_count.cursor.app_error", nil,
				"channel_id="+channelId+", "+err.Error())
		} else {
			result.Data = count
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkChannelStore) GetExtraMembers(channelId string, limit int) StoreChannel {
	storeChannel := make(StoreChannel)

	// SELECT Id, Nickname, Email, ChannelMembers.Roles, Username FROM ChannelMembers, Users
	// WHERE ChannelMembers.UserId = Users.Id AND Users.DeleteAt = 0 AND ChannelId = :ChannelId
	// LIMIT :Limit

	go func() {
		result := StoreResult{}

		var members []model.ExtraMember
		term := rethink.Table("ChannelMembers").EqJoin("UserId", rethink.Table("Users")).
			Without(map[string]string{"left": "Id"}).Zip().
			Filter(rethink.Row.Field("ChannelId").Eq(channelId).And(rethink.Row.Field("DeleteAt").Eq(0)))

		if limit != -1 {
			term.Limit(limit)
		}
		cursor, err := term.Run(s.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkChannelStore.GetExtraMembers",
				"store.rethink_channel.get_extra_members.app_error", nil,
				"channel_id="+channelId+", "+err.Error())
		} else if cursor.All(&members); err != nil {
			result.Err = model.NewLocAppError("RethinkChannelStore.GetExtraMembers",
				"store.rethink_channel.get_extra_members.cursor.app_error", nil,
				"channel_id="+channelId+", "+err.Error())
		} else {
			for i := range members {
				members[i].Sanitize(utils.Cfg.GetSanitizeOptions())
			}
			result.Data = members
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkChannelStore) RemoveMember(channelId string, userId string) StoreChannel {
	storeChannel := make(StoreChannel)

	// DELETE FROM ChannelMembers WHERE ChannelId = :ChannelId AND UserId = :UserId
	go func() {
		result := StoreResult{}

		// Grab the channel we are saving this member to
		if cr := <-s.Get(channelId); cr.Err != nil {
			result.Err = cr.Err
		} else {
			channel := cr.Data.(*model.Channel)

			changed, err := rethink.Table("ChannelMembers").Filter(
				rethink.Row.Field("ChannelId").Eq(channelId).
					And(rethink.Row.Field("UserId").Eq(userId))).
				Delete().RunWrite(s.rethink, runOpts)
			if err != nil {
				result.Err = model.NewLocAppError("RethinkChannelStore.RemoveMember",
					"store.rethink_channel.remove_member.app_error", nil,
					"channel_id="+channelId+", user_id="+userId+", "+err.Error())
			} else if changed.Skipped != 0 {
				result.Err = model.NewLocAppError("RethinkChannelStore.RemoveMember",
					"store.rethink_channel.remove_member.notfound.app_error", nil,
					"channel_id="+channelId+", user_id="+userId+", "+changed.FirstError)
			} else {
				// If sucessfull record members have changed in channel
				if mu := <-s.extraUpdated(channel); mu.Err != nil {
					result.Err = mu.Err
				}
			}
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkChannelStore) PermanentDeleteMembersByUser(userId string) StoreChannel {
	storeChannel := make(StoreChannel)

	// DELETE FROM ChannelMembers WHERE UserId = :UserId", map[string]interface{}{"UserId": userId}); err != nil {
	go func() {
		result := StoreResult{}

		_, err := rethink.Table("ChannelMembers").Filter(
			rethink.Row.Field("UserId").Eq(userId)).
			Delete().RunWrite(s.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkChannelStore.RemoveMember",
				"store.rethink_channel.permanent_delete_members_by_user.app_error", nil,
				"user_id="+userId+", "+err.Error())
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkChannelStore) CheckPermissionsToNoTeam(channelId string, userId string) StoreChannel {
	storeChannel := make(StoreChannel)

	// SELECT COUNT(0) FROM Channels, ChannelMembers WHERE Channels.Id = ChannelMembers.ChannelId
	// AND Channels.DeleteAt = 0 AND ChannelMembers.ChannelId = :ChannelId AND ChannelMembers.UserId = :UserId
	go func() {
		result := StoreResult{}

		var count int64
		cursor, err := rethink.Table("ChannelMembers").EqJoin("ChannelId", rethink.Table("Channels")).
			Without(map[string]string{"left": "Id"}).Zip().Filter(
			rethink.Row.Field("UserId").Eq(userId).And(rethink.Row.Field("DeleteAt").Eq(0)).
				And(rethink.Row.Field("ChannelId").Eq(channelId))).
			Count().Run(s.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkChannelStore.CheckPermissionsTo",
				"store.rethink_channel.check_permissions.app_error", nil,
				"channel_id="+channelId+", user_id="+userId+", "+err.Error())
		} else if err := cursor.One(&count); err != nil {
			result.Err = model.NewLocAppError("RethinkChannelStore.CheckPermissionsTo",
				"store.rethink_channel.check_permissions.cursor.app_error", nil,
				"channel_id="+channelId+", user_id="+userId+", "+err.Error())
		} else {
			result.Data = count
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkChannelStore) CheckPermissionsTo(teamId string, channelId string, userId string) StoreChannel {
	storeChannel := make(StoreChannel)

	//`SELECT COUNT(0) FROM Channels, ChannelMembers WHERE Channels.Id = ChannelMembers.ChannelId
	// AND (Channels.TeamId = :TeamId OR Channels.TeamId = '') AND Channels.DeleteAt = 0
	//  AND ChannelMembers.ChannelId = :ChannelId AND ChannelMembers.UserId = :UserId`,
	go func() {
		result := StoreResult{}

		var count int64
		cursor, err := rethink.Table("ChannelMembers").EqJoin("ChannelId", rethink.Table("Channels")).
			Without(map[string]string{"left": "Id"}).Zip().Filter(
			rethink.Row.Field("UserId").Eq(userId).And(rethink.Row.Field("DeleteAt").Eq(0)).
				And(rethink.Row.Field("ChannelId").Eq(channelId)).
				And(rethink.Row.Field("TeamId").Eq(teamId).Or(rethink.Row.Field("TeamId").Eq("")))).
			Count().Run(s.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkChannelStore.CheckPermissionsTo",
				"store.rethink_channel.check_permissions.app_error", nil,
				"channel_id="+channelId+", user_id="+userId+", "+err.Error())
		} else if err := cursor.One(&count); err != nil {
			result.Err = model.NewLocAppError("RethinkChannelStore.CheckPermissionsTo",
				"store.rethink_channel.check_permissions.cursor.app_error", nil,
				"channel_id="+channelId+", user_id="+userId+", "+err.Error())
		} else {
			result.Data = count
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkChannelStore) CheckPermissionsToByName(teamId string, channelName string, userId string) StoreChannel {
	storeChannel := make(StoreChannel)

	//`SELECT Channels.Id FROM Channels, ChannelMembers WHERE Channels.Id = ChannelMembers.ChannelId
	// AND (Channels.TeamId = :TeamId OR Channels.TeamId = '') AND Channels.Name = :Name
	// AND Channels.DeleteAt = 0 AND ChannelMembers.UserId = :UserId`,
	go func() {
		result := StoreResult{}

		channel := model.Channel{}
		cursor, err := rethink.Table("ChannelMembers").EqJoin("ChannelId", rethink.Table("Channels")).
			Without(map[string]string{"left": "Id"}).Zip().Filter(
			rethink.Row.Field("UserId").Eq(userId).And(rethink.Row.Field("DeleteAt").Eq(0)).
				And(rethink.Row.Field("Name").Eq(channelName)).
				And(rethink.Row.Field("TeamId").Eq(teamId).Or(rethink.Row.Field("TeamId").Eq("")))).
			Run(s.rethink, runOpts)

		if err != nil {
			result.Err = model.NewLocAppError("RethinkChannelStore.CheckPermissionsToByName",
				"store.rethink_channel.check_permissions_by_name.app_error", nil,
				"channel_id="+channelName+", user_id="+userId+", "+err.Error())
		} else if err := cursor.One(&channel); err != nil {
			result.Err = model.NewLocAppError("RethinkChannelStore.CheckPermissionsToByName",
				"store.rethink_channel.check_permissions_by_name.cursor.app_error", nil,
				"channel_id="+channelName+", user_id="+userId+", "+err.Error())
			result.Data = ""
		} else {
			result.Data = channel.Id
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkChannelStore) CheckOpenChannelPermissions(teamId string, channelId string) StoreChannel {
	storeChannel := make(StoreChannel)

	// SELECT COUNT(0) FROM Channels WHERE Channels.Id = :ChannelId
	// AND Channels.TeamId = :TeamId AND Channels.Type = :ChannelType
	go func() {
		result := StoreResult{}

		var count int64
		cursor, err := rethink.Table("Channels").Filter(rethink.Row.Field("Id").Eq(channelId).
			And(rethink.Row.Field("TeamId").Eq(teamId))).
			Count().Run(s.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkChannelStore.CheckOpenChannelPermissions",
				"store.rethink_channel.check_open_channel_permissions.app_error", nil,
				"channel_id="+channelId+", "+err.Error())
		} else if err := cursor.One(&count); err != nil {
			result.Err = model.NewLocAppError("RethinkChannelStore.CheckOpenChannelPermissions",
				"store.rethink_channel.check_open_channel_permissions.count.app_error", nil,
				"channel_id="+channelId+", "+err.Error())
		} else {
			result.Data = count
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkChannelStore) UpdateLastViewedAt(channelId string, userId string) StoreChannel {
	storeChannel := make(StoreChannel)

	// UPDATE ChannelMembers SET MentionCount = 0, MsgCount = Channels.TotalMsgCount,
	// LastViewedAt = Channels.LastPostAt, LastUpdateAt = Channels.LastPostAt
	// FROM Channels WHERE Channels.Id = ChannelMembers.ChannelId AND UserId = :UserId
	// AND ChannelId = :ChannelId`

	go func() {
		channel := model.Channel{}
		result := StoreResult{}

		//var query string
		cursor, err := rethink.Table("ChannelMembers").EqJoin("ChannelId", rethink.Table("Channels")).
			Without(map[string]string{"right": "Id"}).Zip().Filter(
			rethink.Row.Field("UserId").Eq(userId).And(rethink.Row.Field("DeleteAt").Eq(0)).
				And(rethink.Row.Field("ChannelId").Eq(channelId)).
				And(rethink.Row.Field("UserId").Eq(userId))).
			Run(s.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkChannelStore.UpdateLastViewedAt",
				"store.rethink_channel.update_last_viewed_at.app_error", nil,
				"channel_id="+channelId+", user_id="+userId+", "+err.Error())
		} else if cursor.One(&channel); err != nil {
			result.Err = model.NewLocAppError("RethinkChannelStore.UpdateLastViewedAt",
				"store.rethink_channel.update_last_viewed_at.cursor.app_error", nil,
				"channel_id="+channelId+", user_id="+userId+", "+err.Error())
		} else {
			// NOTE: channel.Id here, is actually the ChannelMember.Id
			_, err := rethink.Table("ChannelMembers").Get(channel.Id).
				Update(map[string]interface{}{
					"MentionCount": 0,
					"MsgCount":     channel.TotalMsgCount,
					"LastViewedAt": channel.LastPostAt}).
				RunWrite(s.rethink, runOpts)
			if err != nil {
				result.Err = model.NewLocAppError("RethinkChannelStore.UpdateLastViewedAt",
					"store.rethink_channel.update_last_viewed_at.update.app_error", nil,
					"channel_id="+channelId+", user_id="+userId+", "+err.Error())
			} /*

				TODO thrawn - This *should* return an error if it could not find the item to update,
				but the tests currently expect err != nil if it can't find the item

				 else if changed.Skipped != 0 {
					result.Err = model.NewLocAppError("RethinkChannelStore.UpdateLastViewedAt",
						"store.rethink_channel.update_last_viewed_at.update.notfound.app_error", nil,
						"channel_id="+channelId+", user_id="+userId+", "+changed.FirstError)
				}*/
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkChannelStore) IncrementMentionCount(channelId string, userId string) StoreChannel {
	storeChannel := make(StoreChannel)

	// UPDATE ChannelMembers SET MentionCount = MentionCount + 1 WHERE
	// UserId = :UserId AND ChannelId = :ChannelId
	go func() {
		result := StoreResult{}

		changed, err := rethink.Table("ChannelMembers").Filter(rethink.Row.Field("ChannelId").Eq(channelId).
			And(rethink.Row.Field("UserId").Eq(userId))).Update(map[string]interface{}{
			"MentionCount": rethink.Row.Field("MentionCount").Add(1).Default(0),
		}).RunWrite(s.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkChannelStore.IncrementMentionCount",
				"store.rethink_channel.increment_mention_count.app_error", nil,
				"channel_id="+channelId+", user_id="+userId+", "+err.Error())
		} else if changed.Skipped != 0 {
			result.Err = model.NewLocAppError("RethinkChannelStore.IncrementMentionCount",
				"store.rethink_channel.increment_mention_count.notfound.app_error", nil,
				"channel_id="+channelId+", user_id="+userId+", "+changed.FirstError)
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkChannelStore) GetForExport(teamId string) StoreChannel {
	storeChannel := make(StoreChannel)

	// SELECT * FROM Channels WHERE TeamId = :TeamId AND DeleteAt = 0 AND Type = 'O'
	go func() {
		result := StoreResult{}

		var data []*model.Channel
		cursor, err := rethink.Table("Channels").Filter(rethink.Row.Field("TeamId").Eq(teamId).
			And(rethink.Row.Field("DeletedAt").Eq(0))).And(rethink.Row.Field("Type").Eq("O")).
			Run(s.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkChannelStore.GetAllChannels",
				"store.rethink_channel.get_for_export.app_error", nil,
				"teamId="+teamId+", err="+err.Error())
		} else if err := cursor.All(&data); err != nil {
			result.Err = model.NewLocAppError("RethinkChannelStore.GetAllChannels",
				"store.rethink_channel.get_for_export.cursor.app_error", nil,
				"teamId="+teamId+", err="+err.Error())
		} else {
			result.Data = data
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkChannelStore) AnalyticsTypeCount(teamId string, channelType string) StoreChannel {
	storeChannel := make(StoreChannel)

	// SELECT COUNT(Id) AS Value FROM Channels WHERE Type = :ChannelType AND TeamId = :TeamId
	go func() {
		result := StoreResult{}

		term := rethink.Table("Channels")
		if len(teamId) > 0 {
			term = term.Filter(rethink.Row.Field("Type").Eq(channelType).
				And(rethink.Row.Field("TeamId").Eq(teamId)))
		} else {
			term = term.Filter(rethink.Row.Field("Type").Eq(channelType))
		}

		var count int64
		cursor, err := term.Count().Run(s.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkChannelStore.AnalyticsTypeCount",
				"store.rethink_channel.analytics_type_count.app_error", nil, err.Error())
		} else if err := cursor.One(&count); err != nil {
			result.Err = model.NewLocAppError("RethinkChannelStore.AnalyticsTypeCount",
				"store.rethink_channel.analytics_type_count.cursor.app_error", nil, err.Error())
		} else {
			result.Data = count
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkChannelStore) ExtraUpdateByUser(userId string, time int64) StoreChannel {
	storeChannel := make(StoreChannel)

	// UPDATE Channels SET ExtraUpdateAt = :Time WHERE Id IN
	// (SELECT ChannelId FROM ChannelMembers WHERE UserId = :UserId);`,
	go func() {
		result := StoreResult{}

		channelMembers := make([]*model.ChannelMember, 0)
		cursor, err := rethink.Table("ChannelMembers").
			Filter(rethink.Row.Field("UserId").Eq(userId)).Run(s.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkChannelStore.extraUpdated",
				"store.rethink_channel.extra_updated.app_error", nil, "user_id="+userId+", "+err.Error())
		} else if cursor.All(&channelMembers); err != nil {
			result.Err = model.NewLocAppError("RethinkChannelStore.extraUpdated",
				"store.rethink_channel.extra_updated.cursor.app_error", nil,
				"user_id="+userId+", "+err.Error())
		}

		channelIds := make([]string, len(channelMembers))
		for i := 0; i < len(channelMembers); i++ {
			channelIds[i] = channelMembers[i].ChannelId
		}

		_, err = rethink.Table("Channels").Filter(func(channel rethink.Term) rethink.Term {
			return rethink.Expr(channelIds).Contains(channel.Field("Id")).Not()
		}).Update(map[string]interface{}{"ExtraUpdateAt": time}).RunWrite(s.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkChannelStore.extraUpdated",
				"store.rethink_channel.extra_updated.app_error", nil, "user_id="+userId+", "+err.Error())
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}
