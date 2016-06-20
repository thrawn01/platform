package store

import (
	rethink "github.com/dancannon/gorethink"
	"github.com/mattermost/platform/model"
)

type RethinkEmojiStore struct {
	session *rethink.Session
}

func NewRethinkEmojiStore(session *rethink.Session) EmojiStore {
	store := &RethinkEmojiStore{session}
	store.CreateTablesIfNotExists()
	store.CreateIndexesIfNotExists()
	return store
}

func (self RethinkEmojiStore) UpgradeSchemaIfNeeded() {
}

func (self RethinkEmojiStore) CreateIndexesIfNotExists() {
}

func (self RethinkEmojiStore) CreateTablesIfNotExists() {
	err := rethink.TableCreate("Emojis", rethink.TableCreateOpts{PrimaryKey: "Id"}).Exec(self.session, execOpts)
	handleCreateError("Emoji.CreateTablesIfNotExists().", err)
}

func (self RethinkEmojiStore) Save(emoji *model.Emoji) StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		emoji.PreSave()
		if result.Err = emoji.IsValid(); result.Err != nil {
			storeChannel <- result
			close(storeChannel)
			return
		}

		changed, err := rethink.Table("Emojis").Insert(emoji).RunWrite(self.session, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkEmojiStore.Save",
				"store.rethink_emoji.save.app_error", nil,
				"id="+emoji.Id+", "+err.Error())
		} else if changed.Inserted != 1 {
			result.Err = model.NewLocAppError("RethinkEmojiStore.Save",
				"store.rethink_emoji.save.not_inserted.app_error", nil,
				"id="+emoji.Id+", "+err.Error())
		} else {
			result.Data = emoji
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (self RethinkEmojiStore) Get(id string) StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		var emoji *model.Emoji

		cursor, err := rethink.Table("Emojis").Get(id).Run(self.session, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkEmojiStore.Get",
				"store.rethink_emoji.get.app_error", nil, "id="+id+", "+err.Error())
		} else if err := cursor.One(emoji); err != nil {
			result.Err = model.NewLocAppError("RethinkEmojiStore.Get",
				"store.rethink_emoji.get.cursor.app_error", nil, "id="+id+", "+err.Error())
		} else {
			result.Data = emoji
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (self RethinkEmojiStore) GetByName(name string) StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		var emoji *model.Emoji

		cursor, err := rethink.Table("Emojis").Filter(rethink.Row.Field("Name").Eq(name).
			And(rethink.Row.Field("DeleteAt").Eq(0))).Run(self.session, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkEmojiStore.GetByName",
				"store.rethink_emoji.get_by_name.app_error", nil,
				"name="+name+", "+err.Error())
		} else if err := cursor.One(&emoji); err != nil {
			result.Err = model.NewLocAppError("RethinkEmojiStore.GetByName",
				"store.rethink_emoji.get_by_name.cursor.app_error", nil,
				"name="+name+", "+err.Error())
		} else {
			result.Data = emoji
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (self RethinkEmojiStore) GetAll() StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		var emoji []*model.Emoji

		cursor, err := rethink.Table("Emojis").Run(self.session, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkEmojiStore.Get",
				"store.rethink_emoji.get_all.app_error", nil, err.Error())
		} else if err := cursor.All(emoji); err != nil {
			result.Err = model.NewLocAppError("RethinkEmojiStore.Get",
				"store.rethink_emoji.get_all.cursor.app_error", nil, err.Error())
		} else {
			result.Data = emoji
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (self RethinkEmojiStore) Delete(id string, time int64) StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		changed, err := rethink.Table("Emojis").Get(id).Update(map[string]interface{}{
			"DeleteAt": time,
			"UpdateAt": time,
		}).RunWrite(self.session, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkEmojiStore.Delete",
				"store.rethink_emoji.delete.app_error", nil, "id="+id+", err="+err.Error())
		} else if changed.Updated == 0 {
			result.Err = model.NewLocAppError("RethinkEmojiStore.Delete",
				"store.rethink_emoji.delete.no_results", nil, "id="+id+", err="+err.Error())
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}
