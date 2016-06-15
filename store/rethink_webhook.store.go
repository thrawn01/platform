package store

import (
	rethink "github.com/dancannon/gorethink"
	"github.com/mattermost/platform/model"
)

type RethinkWebhookStore struct {
	rethink *rethink.Session
}

func NewRethinkWebhookStore(session *rethink.Session) WebhookStore {
	s := &RethinkWebhookStore{session}
	s.CreateTablesIfNotExists()
	s.CreateIndexesIfNotExists()
	return s
}

func (s RethinkWebhookStore) CreateTablesIfNotExists() {
	err := rethink.TableCreate("IncomingWebhooks", rethink.TableCreateOpts{PrimaryKey: "Id"}).Exec(s.rethink, execOpts)
	handleCreateError("Teams.CreateTablesIfNotExists()", err)
	err = rethink.TableCreate("OutgoingWebhooks").Exec(s.rethink, execOpts)
	handleCreateError("TeamMembers.CreateTablesIfNotExists()", err)
}

func (s RethinkWebhookStore) CreateIndexesIfNotExists() {
	err := rethink.Table("IncomingWebhooks").IndexCreate("UserId").Exec(s.rethink, execOpts)
	handleCreateError("IncomingWebhooks.CreateIndexesIfNotExists().UserId.IndexCreate", err)
	err = rethink.Table("IncomingWebhooks").IndexWait("UserId").Exec(s.rethink, execOpts)
	handleCreateError("IncomingWebhooks.CreateIndexesIfNotExists().UserId.IndexWait", err)

	err = rethink.Table("IncomingWebhooks").IndexCreate("TeamId").Exec(s.rethink, execOpts)
	handleCreateError("IncomingWebhooks.CreateIndexesIfNotExists().TeamId.IndexCreate", err)
	err = rethink.Table("IncomingWebhooks").IndexWait("TeamId").Exec(s.rethink, execOpts)
	handleCreateError("IncomingWebhooks.CreateIndexesIfNotExists().TeamId.IndexWait", err)

	err = rethink.Table("OutgoingWebhooks").IndexCreate("TeamId").Exec(s.rethink, execOpts)
	handleCreateError("OutgoingWebhooks.CreateIndexesIfNotExists().TeamId.IndexCreate", err)
	err = rethink.Table("OutgoingWebhooks").IndexWait("TeamId").Exec(s.rethink, execOpts)
	handleCreateError("OutgoingWebhooks.CreateIndexesIfNotExists().TeamId.IndexWait", err)
}

func (s RethinkWebhookStore) SaveIncoming(webhook *model.IncomingWebhook) StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		if len(webhook.Id) > 0 {
			result.Err = model.NewLocAppError("RethinkWebhookStore.SaveIncoming",
				"store.rethink_webhooks.save_incoming.existing.app_error", nil, "id="+webhook.Id)
			storeChannel <- result
			close(storeChannel)
			return
		}

		webhook.PreSave()
		if result.Err = webhook.IsValid(); result.Err != nil {
			storeChannel <- result
			close(storeChannel)
			return
		}

		changed, err := rethink.Table("IncomingWebhooks").Insert(webhook).RunWrite(s.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkWebhookStore.SaveIncoming",
				"store.rethink_webhooks.save_incoming.app_error", nil,
				"id="+webhook.Id+", "+err.Error())
		} else if changed.Inserted != 1 {
			result.Err = model.NewLocAppError("RethinkWebhookStore.SaveIncoming",
				"store.rethink_webhooks.save_incoming.not_inserted.app_error", nil,
				"id="+webhook.Id+", "+changed.FirstError)
		} else {
			result.Data = webhook
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkWebhookStore) GetIncoming(id string) StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		var webhook model.IncomingWebhook

		cursor, err := rethink.Table("IncomingWebhooks").Filter(
			rethink.Row.Field("Id").Eq(id).And(rethink.Row.Field("DeleteAt").Eq(0))).
			Run(s.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkWebhookStore.GetIncoming",
				"store.rethink_webhooks.get_incoming.app_error", nil,
				"id="+id+", err="+err.Error())
		} else if err := cursor.One(&webhook); err != nil {
			result.Err = model.NewLocAppError("RethinkWebhookStore.GetIncoming",
				"store.rethink_webhooks.get_incoming.cursor.app_error", nil,
				"id="+id+", err="+err.Error())
		}

		result.Data = &webhook

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkWebhookStore) DeleteIncoming(webhookId string, time int64) StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		_, err := rethink.Table("IncomingWebhooks").Get(webhookId).Update(map[string]interface{}{
			"DeleteAt": time,
			"UpdateAt": time,
		}).RunWrite(s.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkWebhookStore.DeleteIncoming",
				"store.rethink_webhooks.delete_incoming.app_error", nil,
				"id="+webhookId+", err="+err.Error())
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkWebhookStore) PermanentDeleteIncomingByUser(userId string) StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		_, err := rethink.Table("IncomingWebhooks").Filter(
			rethink.Row.Field("UserId").Eq(userId)).Delete().
			RunWrite(s.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkWebhookStore.DeleteIncomingByUser",
				"store.rethink_webhooks.permanent_delete_incoming_by_user.app_error", nil,
				"id="+userId+", err="+err.Error())
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkWebhookStore) GetIncomingByTeam(teamId string) StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		var webhooks []*model.IncomingWebhook

		cursor, err := rethink.Table("IncomingWebhooks").Filter(
			rethink.Row.Field("TeamId").Eq(teamId).And(rethink.Row.Field("DeleteAt").Eq(0))).
			Run(s.rethink, runOpts)

		if err != nil {
			result.Err = model.NewLocAppError("RethinkWebhookStore.GetIncomingByUser",
				"store.rethink_webhooks.get_incoming_by_user.app_error", nil,
				"teamId="+teamId+", err="+err.Error())
		} else if err := cursor.All(&webhooks); err != nil {
			result.Err = model.NewLocAppError("RethinkWebhookStore.GetIncomingByUser",
				"store.rethink_webhooks.get_incoming_by_user.cursor.app_error", nil,
				"teamId="+teamId+", err="+err.Error())
		} else {
			result.Data = webhooks
		}

		if cursor != nil {
			cursor.Close()
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkWebhookStore) GetIncomingByChannel(channelId string) StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		var webhooks []*model.IncomingWebhook

		cursor, err := rethink.Table("IncomingWebhooks").Filter(
			rethink.Row.Field("ChannelId").Eq(channelId).And(rethink.Row.Field("DeleteAt").Eq(0))).
			Run(s.rethink, runOpts)

		if err != nil {
			result.Err = model.NewLocAppError("RethinkWebhookStore.GetIncomingByChannel",
				"store.rethink_webhooks.get_incoming_by_channel.app_error", nil,
				"channelId="+channelId+", err="+err.Error())
		} else if err := cursor.All(&webhooks); err != nil {
			result.Err = model.NewLocAppError("RethinkWebhookStore.GetIncomingByChannel",
				"store.rethink_webhooks.get_incoming_by_channel.cursor.app_error", nil,
				"channelId="+channelId+", err="+err.Error())
		} else {
			result.Data = webhooks
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkWebhookStore) SaveOutgoing(webhook *model.OutgoingWebhook) StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		if len(webhook.Id) > 0 {
			result.Err = model.NewLocAppError("RethinkWebhookStore.SaveOutgoing",
				"store.rethink_webhooks.save_outgoing.override.app_error", nil, "id="+webhook.Id)
			storeChannel <- result
			close(storeChannel)
			return
		}

		webhook.PreSave()
		if result.Err = webhook.IsValid(); result.Err != nil {
			storeChannel <- result
			close(storeChannel)
			return
		}

		changed, err := rethink.Table("OutgoingWebhooks").Insert(webhook).RunWrite(s.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkWebhookStore.SaveOutgoing",
				"store.rethink_webhooks.save_outgoing.app_error", nil,
				"id="+webhook.Id+", "+err.Error())
		} else if changed.Inserted != 1 {
			result.Err = model.NewLocAppError("RethinkWebhookStore.SaveOutgoing",
				"store.rethink_webhooks.save_outgoing.app_error", nil,
				"id="+webhook.Id+", "+err.Error())
		} else {
			result.Data = webhook
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkWebhookStore) GetOutgoing(id string) StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		var webhook model.OutgoingWebhook

		cursor, err := rethink.Table("OutgoingWebhooks").Filter(
			rethink.Row.Field("Id").Eq(id).And(rethink.Row.Field("DeleteAt").Eq(0))).
			Run(s.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkWebhookStore.GetOutgoing",
				"store.rethink_webhooks.get_outgoing.app_error", nil,
				"id="+id+", err="+err.Error())
		} else if err := cursor.One(&webhook); err != nil {
			result.Err = model.NewLocAppError("RethinkWebhookStore.GetOutgoing",
				"store.rethink_webhooks.get_outgoing.cursor.app_error", nil,
				"id="+id+", err="+err.Error())
		} else {
			result.Data = &webhook
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkWebhookStore) GetOutgoingByChannel(channelId string) StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		var webhooks []*model.OutgoingWebhook

		cursor, err := rethink.Table("OutgoingWebhooks").Filter(
			rethink.Row.Field("ChannelId").Eq(channelId).And(rethink.Row.Field("DeleteAt").Eq(0))).
			Run(s.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkWebhookStore.GetOutgoingByChannel",
				"store.rethink_webhooks.get_outgoing_by_channel.app_error", nil,
				"channelId="+channelId+", err="+err.Error())
		} else if err := cursor.All(&webhooks); err != nil {
			result.Err = model.NewLocAppError("RethinkWebhookStore.GetOutgoingByChannel",
				"store.rethink_webhooks.get_outgoing_by_channel.cursor.app_error", nil,
				"channelId="+channelId+", err="+err.Error())
		} else {
			result.Data = webhooks
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkWebhookStore) GetOutgoingByTeam(teamId string) StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		var webhooks []*model.OutgoingWebhook

		cursor, err := rethink.Table("OutgoingWebhooks").Filter(
			rethink.Row.Field("TeamId").Eq(teamId).And(rethink.Row.Field("DeleteAt").Eq(0))).
			Run(s.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkWebhookStore.GetOutgoingByTeam",
				"store.rethink_webhooks.get_outgoing_by_team.app_error", nil,
				"teamId="+teamId+", err="+err.Error())
		} else if err := cursor.All(&webhooks); err != nil {
			result.Err = model.NewLocAppError("RethinkWebhookStore.GetOutgoingByTeam",
				"store.rethink_webhooks.get_outgoing_by_team.cursor.app_error", nil,
				"teamId="+teamId+", err="+err.Error())
		} else {
			result.Data = webhooks
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkWebhookStore) DeleteOutgoing(webhookId string, time int64) StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		_, err := rethink.Table("OutgoingWebhooks").Get(webhookId).Update(map[string]interface{}{
			"DeleteAt": time,
			"UpdateAt": time,
		}).RunWrite(s.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkWebhookStore.DeleteOutgoing",
				"store.rethink_webhooks.delete_outgoing.app_error", nil,
				"id="+webhookId+", err="+err.Error())
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkWebhookStore) PermanentDeleteOutgoingByUser(userId string) StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		_, err := rethink.Table("OutgoingWebhooks").Filter(
			rethink.Row.Field("CreatorId").Eq(userId)).Delete().
			RunWrite(s.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkWebhookStore.DeleteOutgoingByUser",
				"store.rethink_webhooks.permanent_delete_outgoing_by_user.app_error", nil,
				"id="+userId+", err="+err.Error())
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkWebhookStore) UpdateOutgoing(hook *model.OutgoingWebhook) StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		hook.UpdateAt = model.GetMillis()

		changed, err := rethink.Table("OutgoingWebhook").Get(hook.Id).
			Update(hook).RunWrite(s.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkWebhookStore.UpdateOutgoing",
				"store.rethink_webhooks.update_outgoing.app_error", nil,
				"id="+hook.Id+", "+err.Error())
		} else if changed.Updated != 1 {
			result.Err = model.NewLocAppError("RethinkWebhookStore.UpdateOutgoing",
				"store.rethink_webhooks.update_outgoing.app_error", nil,
				"id="+hook.Id+", "+changed.FirstError)
		} else {
			result.Data = hook
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkWebhookStore) AnalyticsIncomingCount(teamId string) StoreChannel {
	storeChannel := make(StoreChannel)

	// SELECT COUNT(*)
	// FROM IncomingWebhooks
	// WHERE DeleteAt = 0 AND TeamId = :TeamId
	go func() {
		result := StoreResult{}

		filter := rethink.Row.Field("DeleteAt")
		if len(teamId) > 0 {
			filter = filter.And(rethink.Row.Field("TeamId").Eq(teamId))
		}

		var count int64
		cursor, err := rethink.Table("IncomingWebhooks").Filter(filter).Count().Run(s.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkWebhookStore.AnalyticsIncomingCount",
				"store.rethink_webhooks.analytics_incoming_count.app_error", nil,
				"team_id="+teamId+", err="+err.Error())
		} else if err := cursor.One(&count); err != nil {
			result.Err = model.NewLocAppError("RethinkWebhookStore.AnalyticsIncomingCount",
				"store.rethink_webhooks.analytics_incoming_count.cursor.app_error", nil,
				"team_id="+teamId+", err="+err.Error())
		} else {
			result.Data = count
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkWebhookStore) AnalyticsOutgoingCount(teamId string) StoreChannel {
	storeChannel := make(StoreChannel)

	// SELECT COUNT(*) FROM OutgoingWebhooks WHERE DeleteAt = 0 AND TeamId = :TeamId
	go func() {
		result := StoreResult{}

		filter := rethink.Row.Field("DeleteAt")
		if len(teamId) > 0 {
			filter = filter.And(rethink.Row.Field("TeamId").Eq(teamId))
		}

		var count int64
		cursor, err := rethink.Table("OutgoingWebhooks").Filter(filter).Count().Run(s.rethink, runOpts)

		if err != nil {
			result.Err = model.NewLocAppError("RethinkWebhookStore.AnalyticsOutgoingCount",
				"store.rethink_webhooks.analytics_outgoing_count.app_error", nil,
				"team_id="+teamId+", err="+err.Error())
		} else if err := cursor.One(&count); err != nil {
			result.Err = model.NewLocAppError("RethinkWebhookStore.AnalyticsOutgoingCount",
				"store.rethink_webhooks.analytics_outgoing_count.cursor.app_error", nil,
				"team_id="+teamId+", err="+err.Error())
		} else {
			result.Data = count
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}
