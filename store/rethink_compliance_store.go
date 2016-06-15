package store

import (
	"strings"

	rethink "github.com/dancannon/gorethink"
	"github.com/mattermost/platform/model"
)

type RethinkComplianceStore struct {
	rethink *rethink.Session
}

func NewRethinkComplianceStore(session *rethink.Session) ComplianceStore {
	s := &RethinkComplianceStore{session}
	s.CreateIndexesIfNotExists()
	s.CreateTablesIfNotExists()
	return s
}

func (s RethinkComplianceStore) UpgradeSchemaIfNeeded() {
}

func (s RethinkComplianceStore) CreateTablesIfNotExists() {
	err := rethink.TableCreate("Compliances", rethink.TableCreateOpts{PrimaryKey: "Id"}).Exec(s.rethink, execOpts)
	handleCreateError("Compliances.CreateTablesIfNotExists()", err)
}

func (s RethinkComplianceStore) CreateIndexesIfNotExists() {
}

func (s RethinkComplianceStore) Save(compliance *model.Compliance) StoreChannel {

	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		compliance.PreSave()
		if result.Err = compliance.IsValid(); result.Err != nil {
			storeChannel <- result
			close(storeChannel)
			return
		}

		changed, err := rethink.Table("Compliances").Insert(compliance).RunWrite(s.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkComplianceStore.Save",
				"store.rethink_compliance.save.saving.app_error", nil, err.Error())
		} else if changed.Inserted == 0 {
			result.Err = model.NewLocAppError("RethinkComplianceStore.Save",
				"store.rethink_compliance.save.saving.not_inserted.app_error", nil, err.Error())
		} else {
			result.Data = compliance
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (us RethinkComplianceStore) Update(compliance *model.Compliance) StoreChannel {

	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		if result.Err = compliance.IsValid(); result.Err != nil {
			storeChannel <- result
			close(storeChannel)
			return
		}

		_, err := rethink.Table("Compliances").Get(compliance.Id).Update(compliance).
			RunWrite(us.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkComplianceStore.Update",
				"store.rethink_compliance.save.saving.app_error", nil, err.Error())
		} else {
			result.Data = compliance
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkComplianceStore) GetAll() StoreChannel {

	storeChannel := make(StoreChannel)

	// SELECT * FROM Compliances ORDER BY CreateAt DESC"
	go func() {
		result := StoreResult{}

		var compliances model.Compliances
		cursor, err := rethink.Table("Compliances").OrderBy(rethink.Desc("CreateAt")).Run(s.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkComplianceStore.Get",
				"store.rethink_compliance.get.finding.app_error", nil, err.Error())
		} else if err := cursor.All(&compliances); err != nil {
			result.Err = model.NewLocAppError("RethinkComplianceStore.Get",
				"store.rethink_compliance.get.cursor.app_error", nil, err.Error())
		} else {
			result.Data = compliances
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (us RethinkComplianceStore) Get(id string) StoreChannel {

	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		var compliance model.Compliance
		cursor, err := rethink.Table("Compliances").Get(id).Run(us.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkComplianceStore.Get",
				"store.rethink_compliance.get.finding.app_error", nil, err.Error())
		} else if cursor.IsNil() {
			result.Err = model.NewLocAppError("RethinkComplianceStore.Get",
				"store.rethink_compliance.get.finding.not_found.app_error", nil, err.Error())
		} else if err := cursor.One(&compliance); err != nil {
			result.Err = model.NewLocAppError("RethinkComplianceStore.Get",
				"store.rethink_compliance.get.finding.cursor.app_error", nil, err.Error())
		} else {
			result.Data = compliance
		}

		storeChannel <- result
		close(storeChannel)

	}()

	return storeChannel
}

func (s RethinkComplianceStore) ComplianceExport(job *model.Compliance) StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		var keywordFilter rethink.Term
		keywords := strings.Fields(strings.TrimSpace(strings.ToLower(strings.Replace(job.Keywords, ",", " ", -1))))
		if len(keywords) > 0 {
			for index, keyword := range keywords {
				if index >= 1 {
					keywordFilter = keywordFilter.And(rethink.Row.Field("Message").Match(keyword))
				} else {
					keywordFilter = rethink.Row.Field("Message").Match(keyword)
				}
			}
		}

		var emailFilter rethink.Term
		emails := strings.Fields(strings.TrimSpace(strings.ToLower(strings.Replace(job.Emails, ",", " ", -1))))
		if len(emails) > 0 {
			for index, email := range emails {
				if index >= 1 {
					emailFilter = emailFilter.And(rethink.Row.Field("Email").Match(email))
				} else {
					emailFilter = rethink.Row.Field("Email").Match(email)
				}
			}
		}

		term := rethink.Table("Posts").EqJoin("UserId", "Users").
			Merge(func(row rethink.Term) interface{} {
				return map[string]interface{}{
					"left": map[string]interface{}{
						"PostFilenames":  row.Field("Filenames"),
						"PostHashtags":   row.Field("Hashtags"),
						"PostProps":      row.Field("Props"),
						"PostType":       row.Field("Type"),
						"PostMessage":    row.Field("Message"),
						"PostOriginalId": row.Field("OriginalId"),
						"PostParentId":   row.Field("ParentId"),
						"PostRootId":     row.Field("RootId"),
						"PostDeleteAt":   row.Field("DeleteAt"),
						"PostUpdateAt":   row.Field("UpdateAt"),
						"PostCreateAt":   row.Field("CreateAt"),
						"PostId":         row.Field("Id"),
						"UserNickname":   row.Field("Nickname"),
						"UserEmail":      row.Field("Email"),
						"UserUsername":   row.Field("Username"),
					},
				}
			}).Without(map[string]string{"left": "Id", "right": "TeamId"}).Zip().
			EqJoin("ChannelId", "Channels").
			Merge(func(row rethink.Term) interface{} {
				return map[string]interface{}{
					"right": map[string]interface{}{
						"ChannelName":        row.Field("Name"),
						"ChannelDisplayName": row.Field("DisplayName"),
					},
				}
			}).Zip().
			EqJoin("TeamId", "Teams").
			Merge(func(row rethink.Term) interface{} {
				return map[string]interface{}{
					"right": map[string]interface{}{
						"TeamName":        row.Field("Name"),
						"TeamDisplayName": row.Field("DisplayName"),
					},
				}
			}).Zip().
			Filter(rethink.Row.Field("PostCreateAt").Gt(job.StartAt).
				And(rethink.Row.Field("PostCreateAt").Le(job.EndAt))).
			And(emailFilter).
			And(keywordFilter).
			OrderBy(rethink.Row.Field("PostCreateAt")).
			Limit(30000)

		var cposts []*model.CompliancePost
		cursor, err := term.Run(s.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkPostStore.ComplianceExport",
				"store.rethink_post.compliance_export.app_error", nil, err.Error())
		} else if err := cursor.All(&cposts); err != nil {
			result.Err = model.NewLocAppError("RethinkPostStore.ComplianceExport",
				"store.rethink_post.compliance_export.cursor.app_error", nil, err.Error())
		} else {
			result.Data = cposts
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}
