package store

import (
	rethink "github.com/dancannon/gorethink"
	"github.com/mattermost/platform/model"
)

type RethinkAuditStore struct {
	rethink *rethink.Session
}

func NewRethinkAuditStore(session *rethink.Session) *RethinkAuditStore {
	s := &RethinkAuditStore{session}
	s.CreateTablesIfNotExists()
	s.CreateIndexesIfNotExists()
	return s
}

func (s RethinkAuditStore) UpgradeSchemaIfNeeded() {
}

func (s RethinkAuditStore) CreateTablesIfNotExists() {
	err := rethink.TableCreate("Audits", rethink.TableCreateOpts{PrimaryKey: "Id"}).Exec(s.rethink, execOpts)
	handleCreateError("Audits.CreateTablesIfNotExists().", err)
}

func (s RethinkAuditStore) CreateIndexesIfNotExists() {
	err := rethink.Table("Audits").IndexCreate("UserId").Exec(s.rethink, execOpts)
	handleCreateError("Audits.CreateIndexesIfNotExists().UserId.IndexCreate", err)
	err = rethink.Table("Audits").IndexWait("UserId").Exec(s.rethink, execOpts)
	handleCreateError("Audits.CreateIndexesIfNotExists().UserId.IndexWait", err)
}

func (s RethinkAuditStore) Save(audit *model.Audit) StoreChannel {

	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		audit.Id = model.NewId()
		audit.CreateAt = model.GetMillis()

		changed, err := rethink.Table("Audits").Insert(audit).RunWrite(s.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkAuditStore.Save",
				"store.sql_audit.save.saving.app_error", nil, "user_id="+
					audit.UserId+" action="+audit.Action)
		} else if changed.Inserted == 0 {
			result.Err = model.NewLocAppError("RethinkAuditStore.Save",
				"store.sql_audit.save.saving.insert.app_error", nil, "user_id="+
					audit.UserId+" action="+audit.Action)
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkAuditStore) Get(user_id string, limit int) StoreChannel {

	storeChannel := make(StoreChannel)

	// SELECT * FROM Audits WHERE UserId = :user_id ORDER BY CreateAt DESC LIMIT :limit
	go func() {
		result := StoreResult{}

		if limit > 1000 {
			limit = 1000
			result.Err = model.NewLocAppError("RethinkAuditStore.Get",
				"store.sql_audit.get.limit.app_error", nil, "user_id="+user_id)
			storeChannel <- result
			close(storeChannel)
			return
		}

		term := rethink.Table("Audits")

		if len(user_id) != 0 {
			term = term.Filter(rethink.Row.Field("UserId").Eq(user_id))
		}

		term = term.OrderBy(rethink.Desc("CreateAt")).Limit(limit)

		var audits model.Audits
		cursor, err := term.Run(s.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkAuditStore.Get",
				"store.sql_audit.get.finding.app_error", nil, "user_id="+user_id)
		} else if cursor.All(&audits); err != nil {
			result.Err = model.NewLocAppError("RethinkAuditStore.Get",
				"store.sql_audit.get.finding.app_error", nil, "user_id="+user_id)
		} else {
			result.Data = audits
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkAuditStore) PermanentDeleteByUser(userId string) StoreChannel {

	storeChannel := make(StoreChannel)

	// DELETE FROM Audits WHERE UserId = :userId
	go func() {
		result := StoreResult{}

		err := rethink.Table("Audits").Filter(rethink.Row.Field("UserId").Eq(userId)).
			Delete().Exec(s.rethink, execOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkAuditStore.Delete", "store.sql_audit.permanent_delete_by_user.app_error", nil, "user_id="+userId)
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}
