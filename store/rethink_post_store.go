package store

import (
	"fmt"
	"strings"

	l4g "github.com/alecthomas/log4go"
	rethink "github.com/dancannon/gorethink"
	"github.com/mattermost/platform/model"
)

type RethinkPostStore struct {
	rethink *rethink.Session
}

func NewRethinkPostStore(session *rethink.Session) PostStore {
	s := &RethinkPostStore{session}
	s.CreateIndexesIfNotExists()
	s.CreateTablesIfNotExists()
	return s
}

func (s RethinkPostStore) UpgradeSchemaIfNeeded() {
}

func (s RethinkPostStore) CreateTablesIfNotExists() {
	err := rethink.TableCreate("Posts", rethink.TableCreateOpts{PrimaryKey: "Id"}).Exec(s.rethink, execOpts)
	handleCreateError("Posts.CreateTablesIfNotExists()", err)
}

func (s RethinkPostStore) CreateIndexesIfNotExists() {
	handleCreateError("Posts.UpdateAt.CreateIndexesIfNotExists().IndexCreate",
		rethink.Table("Posts").IndexCreate("UpdateAt").Exec(s.rethink, execOpts))
	handleCreateError("Commands.UpdateAt.CreateIndexesIfNotExists().IndexWait",
		rethink.Table("Posts").IndexWait("UpdateAt").Exec(s.rethink, execOpts))

	handleCreateError("Posts.CreateAt.CreateIndexesIfNotExists().IndexCreate",
		rethink.Table("Posts").IndexCreate("CreateAt").Exec(s.rethink, execOpts))
	handleCreateError("Commands.CreateAt.CreateIndexesIfNotExists().IndexWait",
		rethink.Table("Posts").IndexWait("CreateAt").Exec(s.rethink, execOpts))

	handleCreateError("Posts.ChannelId.CreateIndexesIfNotExists().IndexCreate",
		rethink.Table("Posts").IndexCreate("ChannelId").Exec(s.rethink, execOpts))
	handleCreateError("Commands.ChannelId.CreateIndexesIfNotExists().IndexWait",
		rethink.Table("Posts").IndexWait("ChannelId").Exec(s.rethink, execOpts))

	handleCreateError("Posts.RootId.CreateIndexesIfNotExists().IndexCreate",
		rethink.Table("Posts").IndexCreate("RootId").Exec(s.rethink, execOpts))
	handleCreateError("Commands.RootId.CreateIndexesIfNotExists().IndexWait",
		rethink.Table("Posts").IndexWait("RootId").Exec(s.rethink, execOpts))
}

func (s RethinkPostStore) Save(post *model.Post) StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		if len(post.Id) > 0 {
			result.Err = model.NewLocAppError("RethinkPostStore.Save",
				"store.rethink_post.save.existing.app_error", nil, "id="+post.Id)
			storeChannel <- result
			close(storeChannel)
			return
		}

		post.PreSave()
		if result.Err = post.IsValid(); result.Err != nil {
			storeChannel <- result
			close(storeChannel)
			return
		}

		changed, err := rethink.Table("Posts").Insert(post).RunWrite(s.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkPostStore.Save",
				"store.rethink_post.save.app_error", nil, "id="+post.Id+", "+err.Error())
		} else if changed.Inserted == 0 {
			result.Err = model.NewLocAppError("RethinkPostStore.Save",
				"store.rethink_post.save.insert.app_error", nil, "id="+post.Id+", "+changed.FirstError)
		} else {
			time := model.GetMillis()

			if post.Type != model.POST_JOIN_LEAVE {
				s.updateTotalMsgCount(post, time)
			} else {
				// don't update TotalMsgCount for unimportant messages
				// so that the channel isn't marked as unread
				s.updateLastPostAt(post, time)
			}

			if len(post.RootId) > 0 {
				s.updateRootPost(post, time)
			}

			result.Data = post
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkPostStore) updateTotalMsgCount(post *model.Post, time int64) {
	updated, err := rethink.Table("Channels").Get(post.ChannelId).
		Update(map[string]interface{}{
			"LastPostAt":    time,
			"TotalMsgCount": rethink.Row.Field("MentionCount").Add(1).Default(0),
		}).RunWrite(s.rethink, runOpts)
	if err != nil {
		l4g.Error(model.NewLocAppError("RethinkPostStore.updateTotalMsgCount",
			"msg_count.app_error", nil, "id="+post.Id+", "+err.Error()).Error())
	} else if updated.Skipped != 0 {
		l4g.Error(model.NewLocAppError("RethinkPostStore.updateTotalMsgCount",
			"not_found.app_error", nil, "id="+post.Id+", "+updated.FirstError).Error())

	}
}

func (s RethinkPostStore) updateLastPostAt(post *model.Post, time int64) {
	updated, err := rethink.Table("Channels").Get(post.ChannelId).
		Update(map[string]interface{}{
			"LastPostAt": time,
		}).RunWrite(s.rethink, runOpts)
	if err != nil {
		l4g.Error(model.NewLocAppError("RethinkPostStore.updateLastPostAt",
			"last_post.app_error", nil, "id="+post.Id+", "+err.Error()).Error())
	} else if updated.Skipped != 0 {
		l4g.Error(model.NewLocAppError("RethinkPostStore.updateLastPostAt",
			"last_post.not_found.app_error", nil,
			"id="+post.Id+", "+updated.FirstError).Error())
	}
}

func (s RethinkPostStore) updateRootPost(post *model.Post, time int64) {
	updated, err := rethink.Table("Posts").Get(post.RootId).
		Update(map[string]interface{}{
			"UpdateAt": time,
		}).RunWrite(s.rethink, runOpts)
	if err != nil {
		l4g.Error(model.NewLocAppError("RethinkPostStore.updateRootPost",
			"update_at.app_error", nil,
			"id="+post.Id+", "+err.Error()).Error())
	} else if updated.Skipped != 0 {
		l4g.Error(model.NewLocAppError("RethinkPostStore.updateRootPost",
			"update_at.not_found.app_error", nil,
			"id="+post.Id+", "+updated.FirstError).Error())
	}
}

func (s RethinkPostStore) Update(oldPost *model.Post, newMessage string, newHashtags string) StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		editPost := *oldPost
		editPost.Message = newMessage
		editPost.UpdateAt = model.GetMillis()
		editPost.Hashtags = newHashtags

		oldPost.DeleteAt = editPost.UpdateAt
		oldPost.UpdateAt = editPost.UpdateAt
		oldPost.OriginalId = oldPost.Id
		oldPost.Id = model.NewId()

		if result.Err = editPost.IsValid(); result.Err != nil {
			storeChannel <- result
			close(storeChannel)
			return
		}

		changed, err := rethink.Table("Posts").Get(editPost.Id).Update(&editPost).RunWrite(s.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkPostStore.Update",
				"store.rethink_post.update.app_error", nil, "id="+editPost.Id+", "+err.Error())
		} else if changed.Updated == 0 {
			result.Err = model.NewLocAppError("RethinkPostStore.Update",
				"store.rethink_post.update.not_found.app_error",
				nil, "id="+editPost.Id+", "+changed.FirstError)
		} else {
			time := model.GetMillis()

			// TODO: This is not efficient when editing root posts
			s.updateLastPostAt(&editPost, time)

			if len(editPost.RootId) > 0 {
				s.updateRootPost(&editPost, time)
			}

			// insert the old post as deleted
			changed, err := rethink.Table("Posts").Insert(oldPost).RunWrite(s.rethink, runOpts)
			if err != nil {
				result.Err = model.NewLocAppError("RethinkPostStore.Save",
					"store.rethink_post.insert.oldpost.app_error", nil,
					"id="+oldPost.Id+", "+err.Error())
			} else if changed.Updated == 0 {
				result.Err = model.NewLocAppError("RethinkPostStore.Save",
					"store.rethink_post.insert.oldpost.app_error", nil,
					"id="+oldPost.Id+", "+changed.FirstError)
			}

			result.Data = &editPost
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkPostStore) Get(id string) StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}
		pl := &model.PostList{}

		var post model.Post
		// SELECT * FROM Posts WHERE Id = :Id AND DeleteAt = 0
		cursor, err := rethink.Table("Posts").Get(id).Run(s.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkPostStore.GetPost",
				"store.rethink_post.get.app_error", nil, "id="+id+err.Error())
		} else if err := cursor.One(&post); err != nil {
			result.Err = model.NewLocAppError("RethinkPostStore.GetPost",
				"store.rethink_post.get.cursor.app_error", nil, "id="+id+err.Error())
		}

		pl.AddPost(&post)
		pl.AddOrder(id)

		rootId := post.RootId

		if rootId == "" {
			rootId = post.Id
		}

		var posts []*model.Post
		// SELECT * FROM Posts WHERE (Id = :Id OR RootId = :RootId) AND DeleteAt = 0
		cursor, err = rethink.Table("Posts").Filter(rethink.Row.Field("Id").Eq(rootId).
			Or(rethink.Row.Field("RootId").Eq(rootId)).
			And(rethink.Row.Field("DeleteAt").Eq(0))).Run(s.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkPostStore.GetPost",
				"store.rethink_post.get.app_error", nil, "root_id="+rootId+err.Error())
		} else if err := cursor.All(&posts); err != nil {
			result.Err = model.NewLocAppError("RethinkPostStore.GetPost",
				"store.rethink_post.get.cursor.app_error", nil, "root_id="+rootId+err.Error())
		} else {
			for _, p := range posts {
				pl.AddPost(p)
			}
		}

		if cursor != nil {
			cursor.Close()
		}

		result.Data = pl
		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

type etagPosts struct {
	Id       string
	UpdateAt int64
}

func (s RethinkPostStore) GetEtag(channelId string) StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		var et etagPosts
		// SELECT Id, UpdateAt FROM Posts WHERE ChannelId = :ChannelId ORDER BY UpdateAt DESC LIMIT 1
		cursor, err := rethink.Table("Posts").Filter(rethink.Row.Field("ChannelId").Eq(channelId)).
			OrderBy(rethink.Desc("UpdateAt")).Limit(1).Run(s.rethink, runOpts)
		if err != nil {
			l4g.Error(model.NewLocAppError("RethinkPostStore.GetEtag",
				"query.app_error", nil, "id="+channelId+", "+err.Error()).Error())
			result.Data = fmt.Sprintf("%v.0.%v", model.CurrentVersion, model.GetMillis())
		} else if err := cursor.One(&et); err != nil {
			l4g.Error(model.NewLocAppError("RethinkPostStore.GetEtag",
				"query.cursor.app_error", nil, "id="+channelId+", "+err.Error()).Error())
			result.Data = fmt.Sprintf("%v.0.%v", model.CurrentVersion, model.GetMillis())
		} else {
			result.Data = fmt.Sprintf("%v.%v.%v", model.CurrentVersion, et.Id, et.UpdateAt)
		}

		if cursor != nil {
			cursor.Close()
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkPostStore) Delete(postId string, time int64) StoreChannel {
	storeChannel := make(StoreChannel)

	//Update Posts SET DeleteAt = :DeleteAt, UpdateAt = :UpdateAt
	// WHERE Id = :Id OR ParentId = :ParentId OR RootId = :RootId
	go func() {
		result := StoreResult{}

		_, err := rethink.Table("Posts").Filter(
			rethink.Row.Field("Id").Eq(postId).
				And(rethink.Row.Field("ParentId").Eq(postId).
					And(rethink.Row.Field("RootId").Eq(postId)))).
			Update(map[string]interface{}{"DeleteAt": time, "UpdateAt": time}).
			RunWrite(s.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkPostStore.Delete",
				"store.rethink_post.delete.app_error", nil, "id="+postId+", err="+err.Error())
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkPostStore) permanentDelete(postId string) StoreChannel {
	storeChannel := make(StoreChannel)

	// DELETE FROM Posts WHERE Id = :Id OR ParentId = :ParentId OR RootId = :RootId
	go func() {
		result := StoreResult{}

		err := rethink.Table("Posts").Get(postId).Delete().Exec(s.rethink, execOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkPostStore.Delete",
				"store.rethink_post.permanent_delete.app_error", nil,
				"id="+postId+", err="+err.Error())
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkPostStore) permanentDeleteAllCommentByUser(userId string) StoreChannel {
	storeChannel := make(StoreChannel)

	// DELETE FROM Posts WHERE UserId = :UserId AND RootId != ''"
	go func() {
		result := StoreResult{}

		err := rethink.Table("Posts").Filter(rethink.Row.Field("UserId").Eq(userId).
			And(rethink.Row.Field("RootId").Ne(""))).Delete().Exec(s.rethink, execOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkPostStore.permanentDeleteAllCommentByUser",
				"store.rethink_post.permanent_delete_all_comments_by_user.app_error", nil,
				"userId="+userId+", err="+err.Error())
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkPostStore) PermanentDeleteByUser(userId string) StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		// First attempt to delete all the comments for a user
		if r := <-s.permanentDeleteAllCommentByUser(userId); r.Err != nil {
			result.Err = r.Err
			storeChannel <- result
			close(storeChannel)
			return
		}

		// Now attempt to delete all the root posts for a user.  This will also
		// delete all the comments for each post.
		found := true
		count := 0

		// SELECT Id FROM Posts WHERE UserId = :UserId LIMIT 1000
		for found {
			var posts []model.Post
			cursor, err := rethink.Table("Posts").Filter(rethink.Row.Field("UserId").Eq(userId)).
				Limit(1000).Run(s.rethink, runOpts)
			if err != nil {
				result.Err = model.NewLocAppError("RethinkPostStore.PermanentDeleteByUser.select",
					"store.rethink_post.permanent_delete_by_user.app_error", nil,
					"userId="+userId+", err="+err.Error())
				storeChannel <- result
				close(storeChannel)
				return
			} else if err := cursor.All(&posts); err != nil {
				result.Err = model.NewLocAppError("RethinkPostStore.PermanentDeleteByUser.select",
					"store.rethink_post.permanent_delete_by_user.app_error", nil,
					"userId="+userId+", err="+err.Error())
				storeChannel <- result
				close(storeChannel)
				if cursor != nil {
					cursor.Close()
				}
				return

			} else {
				found = false
				for _, post := range posts {
					found = true
					if r := <-s.permanentDelete(post.Id); r.Err != nil {
						result.Err = r.Err
						storeChannel <- result
						close(storeChannel)
						return
					}
				}
			}

			// This is a fail safe, give up if more than 10K messages
			count = count + 1
			if count >= 10 {
				result.Err = model.NewLocAppError("RethinkPostStore.PermanentDeleteByUser.toolarge",
					"store.rethink_post.permanent_delete_by_user.too_many.app_error",
					nil, "userId="+userId)
				storeChannel <- result
				close(storeChannel)
				return
			}
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkPostStore) GetPosts(channelId string, offset int, limit int) StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		if limit > 1000 {
			result.Err = model.NewLocAppError("RethinkPostStore.GetLinearPosts",
				"store.rethink_post.get_posts.app_error", nil, "channelId="+channelId)
			storeChannel <- result
			close(storeChannel)
			return
		}

		rpc := s.getRootPosts(channelId, offset, limit)
		cpc := s.getParentsPosts(channelId, offset, limit)

		var rpr, cpr StoreResult
		if rpr = <-rpc; rpr.Err != nil {
			result.Err = rpr.Err
		} else if cpr = <-cpc; cpr.Err != nil {
			result.Err = cpr.Err
		} else {
			posts := rpr.Data.([]*model.Post)
			parents := cpr.Data.([]*model.Post)

			list := &model.PostList{Order: make([]string, 0, len(posts))}

			for _, p := range posts {
				list.AddPost(p)
				list.AddOrder(p.Id)
			}

			for _, p := range parents {
				list.AddPost(p)
			}

			list.MakeNonNil()

			result.Data = list
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

/* var id = "ba51d907-0a96-4b2e-8693-9286cc41dab6";
//var id = "12280be8-12f8-436c-8d72-c689b6259ee7";
r.db('mattermost').table('Posts').filter({"id": id}).union(
	r.db('mattermost').table('Posts').filter(function(doc) {
		 return r.db('mattermost').table('Posts')
		  .filter({"id": id})
		  .pluck("RootId").distinct()
	    .contains(function (row) {
	      return row("RootId").eq(doc("id")).or(row("RootId").eq(doc("RootId")));
	    });
	})
); */

func (s RethinkPostStore) GetPostsSince(channelId string, time int64) StoreChannel {
	storeChannel := make(StoreChannel)

	// (SELECT * FROM Posts WHERE (UpdateAt > :Time AND ChannelId = :ChannelId) LIMIT 1000)
	// UNION (SELECT * FROM Posts WHERE Id IN
	// (SELECT * FROM
	// (SELECT RootId FROM Posts WHERE UpdateAt > :Time AND ChannelId = :ChannelId LIMIT 1000) temp_tab))
	// ORDER BY CreateAt DESC

	go func() {
		result := StoreResult{}

		var posts []*model.Post

		filter := rethink.Row.Field("UpdateAt").Gt(time).
			And(rethink.Row.Field("ChannelId").Eq(channelId))

		term := rethink.Table("Posts").Filter(filter).
			Union(rethink.Table("Posts").Filter(func(doc rethink.Term) rethink.Term {
				return rethink.Table("Posts").Filter(
					filter).Pluck("RootId").Distinct().
					Contains(func(row rethink.Term) rethink.Term {
						return row.Field("RootId").Eq(doc.Field("Id")).
							Or(row.Field("RootId").Eq(doc.Field("RootId")))
					})
			}).OrderBy(rethink.Desc("CreateAt"))).Distinct()

		cursor, err := term.Run(s.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkPostStore.GetPostsSince",
				"store.rethink_post.get_posts_since.app_error", nil,
				"channelId="+channelId+err.Error())
		} else if err := cursor.All(&posts); err != nil {
			result.Err = model.NewLocAppError("RethinkPostStore.GetPostsSince",
				"store.rethink_post.get_posts_since.cursor.app_error", nil,
				"channelId="+channelId+err.Error())
		} else {
			list := &model.PostList{Order: make([]string, 0, len(posts))}

			for _, p := range posts {
				list.AddPost(p)
				if p.UpdateAt > time {
					list.AddOrder(p.Id)
				}
			}
			result.Data = list
		}

		if cursor != nil {
			cursor.Close()
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkPostStore) GetPostsBefore(channelId string, postId string, numPosts int, offset int) StoreChannel {
	return s.getPostsAround(channelId, postId, numPosts, offset, true)
}

func (s RethinkPostStore) GetPostsAfter(channelId string, postId string, numPosts int, offset int) StoreChannel {
	return s.getPostsAround(channelId, postId, numPosts, offset, false)
}

func (s RethinkPostStore) getPostsAround(channelId string, postId string, numPosts int, offset int, before bool) StoreChannel {
	storeChannel := make(StoreChannel)

	// (SELECT * FROM Posts WHERE (CreateAt `+direction+` (
	//	SELECT CreateAt FROM Posts WHERE Id = :PostId)
	//        AND ChannelId = :ChannelId
	//		AND DeleteAt = 0)
	// ORDER BY CreateAt `+sort+` LIMIT :NumPosts OFFSET :Offset)`,

	//(SELECT * FROM Posts WHERE Id IN (SELECT * FROM (SELECT RootId FROM Posts WHERE
	//	(CreateAt `+direction+` (SELECT CreateAt FROM Posts WHERE Id = :PostId)
	//		AND ChannelId = :ChannelId AND DeleteAt = 0)
	//		ORDER BY CreateAt `+sort+` LIMIT :NumPosts OFFSET :Offset) temp_tab))
	// ORDER BY CreateAt DESC`,
	go func() {
		result := StoreResult{}

		var post model.Post
		cursor, err := rethink.Table("Posts").Get(postId).Run(s.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkPostStore.getPostsAround",
				"store.rethink_post.get.app_error", nil, "id="+postId+err.Error())
		} else if err := cursor.One(&post); err != nil {
			result.Err = model.NewLocAppError("RethinkPostStore.getPostsAround",
				"store.rethink_post.get.cursor.app_error", nil, "id="+postId+err.Error())
		}

		if result.Err != nil {
			storeChannel <- result
			close(storeChannel)
			return
		}

		if cursor != nil {
			cursor.Close()
		}

		var posts []*model.Post

		var filter rethink.Term
		var query rethink.Term
		if before {
			filter = rethink.Row.Field("CreateAt").Lt(post.CreateAt).
				And(rethink.Row.Field("ChannelId").Eq(channelId)).
				And(rethink.Row.Field("DeleteAt").Eq(0))
			query = rethink.Table("Posts").Filter(filter).
				Limit(numPosts).OffsetsOf(offset).OrderBy(rethink.Desc("CreateAt"))
		} else {
			filter = rethink.Row.Field("CreateAt").Gt(post.CreateAt).
				And(rethink.Row.Field("ChannelId").Eq(channelId)).
				And(rethink.Row.Field("DeleteAt").Eq(0))
			query = rethink.Table("Posts").Filter(filter).
				Limit(numPosts).OffsetsOf(offset).OrderBy(rethink.Asc("CreateAt"))
		}

		cursor, err = query.Union(rethink.Table("Posts").Filter(func(doc rethink.Term) rethink.Term {
			return query.Pluck("RootId").Distinct().
				Contains(func(row rethink.Term) rethink.Term {
					return row.Field("RootId").Eq(doc.Field("Id")).
						Or(row.Field("RootId").Eq(doc.Field("RootId")))
				})
		}).Distinct().OrderBy(rethink.Desc("CreateAt"))).Run(s.rethink, runOpts)

		query = query.Limit(numPosts).OffsetsOf(offset)

		if err != nil {
			result.Err = model.NewLocAppError("RethinkPostStore.GetPostContext",
				"store.rethink_post.get_posts_around.get.app_error", nil,
				"channelId="+channelId+err.Error())
		} else if err := cursor.All(&posts); err != nil {
			result.Err = model.NewLocAppError("RethinkPostStore.GetPostContext",
				"store.rethink_post.get_posts_around.cursor.get.app_error", nil,
				"channelId="+channelId+err.Error())
		} else {
			list := &model.PostList{Order: make([]string, 0, len(posts))}

			// We need to flip the order if we selected backwards
			if before {
				for _, p := range posts {
					list.AddPost(p)
					list.AddOrder(p.Id)
				}
			} else {
				l := len(posts)
				for i := range posts {
					list.AddPost(posts[l-i-1])
					list.AddOrder(posts[l-i-1].Id)
				}
			}

			result.Data = list
		}

		if cursor != nil {
			cursor.Close()
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkPostStore) getRootPosts(channelId string, offset int, limit int) StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		// SELECT * FROM Posts WHERE ChannelId = :ChannelId AND DeleteAt = 0
		// ORDER BY CreateAt DESC LIMIT :Limit OFFSET :Offset
		var posts []*model.Post
		cursor, err := rethink.Table("Posts").Filter(
			rethink.Row.Field("ChannelId").Eq(channelId).
				And(rethink.Row.Field("DeleteAt").Eq(0))).
			OrderBy(rethink.Desc("CreateAt")).
			Limit(limit).OffsetsOf(offset).Run(s.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkPostStore.GetLinearPosts",
				"store.rethink_post.get_root_posts.app_error", nil,
				"channelId="+channelId+err.Error())
		} else if err := cursor.All(&posts); err != nil {
			result.Err = model.NewLocAppError("RethinkPostStore.GetLinearPosts",
				"store.rethink_post.get_root_posts.cursor.app_error", nil,
				"channelId="+channelId+err.Error())
		} else {
			result.Data = posts
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkPostStore) getParentsPosts(channelId string, offset int, limit int) StoreChannel {
	storeChannel := make(StoreChannel)

	// SELECT q2.* FROM Posts q2 INNER JOIN
	//     (SELECT DISTINCT q3.RootId FROM
	//         (SELECT RootId FROM Posts WHERE ChannelId = :ChannelId1 AND DeleteAt = 0
	// 	 ORDER BY CreateAt DESC LIMIT :Limit OFFSET :Offset) q3
	//     WHERE q3.RootId != '') q1
	// ON q1.RootId = q2.Id OR q1.RootId = q2.RootId
	// WHERE ChannelId = :ChannelId2 AND DeleteAt = 0
	// ORDER BY CreateAt

	go func() {
		result := StoreResult{}

		var posts []*model.Post
		query := rethink.Table("Posts").Filter(rethink.Row.Field("DeleteAt").Eq(0).
			And(rethink.Row.Field("ChannelId").Eq(channelId))).Limit(limit).OffsetsOf(offset)

		term := rethink.Table("Posts").Filter(func(doc rethink.Term) rethink.Term {
			return query.Pluck("RootId").Distinct().
				Contains(func(row rethink.Term) rethink.Term {
					return row.Field("RootId").Eq(doc.Field("Id")).
						Or(row.Field("RootId").Eq(doc.Field("RootId")))
				})
		}).OrderBy(rethink.Desc("CreateAt")).Distinct()
		cursor, err := term.Run(s.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkPostStore.GetLinearPosts",
				"store.rethink_post.get_parents_posts.app_error", nil,
				"channelId="+channelId+err.Error())
		} else if err := cursor.All(&posts); err != nil {
			result.Err = model.NewLocAppError("RethinkPostStore.GetLinearPosts",
				"store.rethink_post.get_parents_posts.cursor.app_error", nil,
				"channelId="+channelId+err.Error())
		} else {
			result.Data = posts
		}

		if cursor != nil {
			cursor.Close()
		}

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

var specialSearchChar = []string{
	"<",
	">",
	"+",
	"-",
	"(",
	")",
	"~",
	"@",
	":",
}

func (s RethinkPostStore) fetchChannelIds(channelFilter rethink.Term) ([]string, error) {
	results := make([]string, 0)
	cursor, err := rethink.Table("ChannelMembers").EqJoin("ChannelId", rethink.Table("Channels")).
		Without(map[string]string{"left": "Id"}).
		Zip().Filter(channelFilter).Run(s.rethink, runOpts)
	if err != nil {
		return nil, model.NewLocAppError("RethinkPostStore.Search",
			"fetchChannelIds.app_error", nil, err.Error())
	} else if cursor.Err() != nil {
		return nil, model.NewLocAppError("RethinkPostStore.Search",
			"fetchChannelIds.cursor.app_error", nil, err.Error())
	}
	if cursor != nil {
		var row map[string]interface{}
		for cursor.Next(&row) {
			results = append(results, row["Id"])
		}
		cursor.Close()
	}
	return results, nil
}

func (s RethinkPostStore) fetchUserIds(filter rethink.Term) ([]string, error) {
	results := make([]string, 0)
	cursor, err := rethink.Table("Users").Filter(filter).Run(s.rethink, runOpts)
	if err != nil {
		return nil, model.NewLocAppError("RethinkPostStore.Search",
			"fetchUserIds.app_error", nil, err.Error())
	} else if cursor.Err() != nil {
		return nil, model.NewLocAppError("RethinkPostStore.Search",
			"fetchUserIds.cursor.app_error", nil, err.Error())
	}
	if cursor != nil {
		var row map[string]interface{}
		for cursor.Next(&row) {
			results = append(results, row["Id"])
		}
		cursor.Close()
	}
	return results, nil
}

func (s RethinkPostStore) Search(teamId string, userId string, params *model.SearchParams) StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		termMap := map[string]bool{}
		terms := params.Terms

		if terms == "" && len(params.InChannels) == 0 && len(params.FromUsers) == 0 {
			result.Data = []*model.Post{}
			storeChannel <- result
			return
		}

		searchType := "Message"
		if params.IsHashtag {
			searchType = "Hashtags"
			for _, term := range strings.Split(terms, " ") {
				termMap[strings.ToUpper(term)] = true
			}
		}

		// these chars have special meaning and can be treated as spaces
		for _, c := range specialSearchChar {
			terms = strings.Replace(terms, c, " ", -1)
		}

		var posts []*model.Post

		var channelIds, userIds []string
		var err error
		channelFilter := rethink.Row.Field("UserId").Eq(userId).
			And(rethink.Row.Field("DeleteAt").Eq(0)).
			And(rethink.Row.Field("TeamId").Eq(teamId)).
			Or(rethink.Row.Field("TeamId").Eq(""))
		if len(params.InChannels) != 0 {
			channelFilter = channelFilter.And(rethink.Row.Field("Name").Eq(params.InChannels[0]))
			if len(params.InChannels) > 1 {
				for i := 1; i < len(params.InChannels); i++ {
					channelFilter = channelFilter.And(rethink.Row.Field("Name").
						Eq(params.InChannels[i]))
				}
			}
			channelIds, err = s.fetchChannelIds(channelFilter)
			if err != nil {
				result.Err = err
				storeChannel <- result
				close(storeChannel)
			}
		}

		if len(params.FromUsers) != 0 {
			userFilter := rethink.Row.Field("Username").Eq(params.FromUsers[0])
			if len(params.FromUsers) > 1 {
				for i := 1; i < len(params.FromUsers); i++ {
					userFilter = userFilter.And(rethink.Row.Field("Username").
						Eq(params.FromUsers[i]))
				}
			}
			userIds, err = s.fetchUserIds(userFilter)
			if err != nil {
				result.Err = err
				storeChannel <- result
				close(storeChannel)
			}
		}

		var searchFilter rethink.Term

		if terms != "" {
			splitTerms := strings.Fields(terms)
			for i, t := range strings.Fields(terms) {
				splitTerms[i] = "+" + t
			}

			// OR
			if params.OrTerms {
				searchFilter = rethink.Row.Field(searchType).Eq(splitTerms[0])
				if len(params.FromUsers) > 1 {
					for i := 1; i < len(splitTerms); i++ {
						searchFilter = searchFilter.And(rethink.Row.Field(searchType).
							Match(splitTerms[i]))
					}
				}
			} else {
				// AND
				searchFilter = rethink.Row.Field(searchType).Eq(splitTerms[0])
				if len(params.FromUsers) > 1 {
					for i := 1; i < len(splitTerms); i++ {
						searchFilter = searchFilter.Or(rethink.Row.Field(searchType).
							Match(splitTerms[i]))
					}
				}
			}
		}

		//SELECT * FROM Posts WHERE DeleteAt = 0 AND Type NOT LIKE '` + model.POST_SYSTEM_MESSAGE_PREFIX + `%'
		//   POST_FILTER AND ChannelId IN (SELECT Id FROM Channels, ChannelMembers
		//                               WHERE Id = ChannelId AND (TeamId = :TeamId OR TeamId = '')
		//					AND UserId = :UserId AND DeleteAt = 0 CHANNEL_FILTER)
		// SEARCH_CLAUSE
		//	ORDER BY CreateAt DESC
		// LIMIT 100`

		cursor, err := rethink.Table("Posts").
			Filter(searchFilter).
			Filter(func(row rethink.Term) rethink.Term {
				return rethink.Expr(channelIds).Contains(row.Field("ChannelId"))
			}).
			Filter(func(row rethink.Term) rethink.Term {
				return rethink.Expr(userIds).Contains(row.Field("UserId"))
			}).
			OrderBy(rethink.Desc("CreateAt")).Limit(100).Run(s.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkPostStore.Search",
				"store.rethink_post.search.app_error", nil, "teamId="+teamId+", err="+err.Error())
		} else if err := cursor.All(&posts); err != nil {
			result.Err = model.NewLocAppError("RethinkPostStore.Search",
				"store.rethink_post.search.app_error", nil, "teamId="+teamId+", err="+err.Error())
		}

		list := &model.PostList{Order: make([]string, 0, len(posts))}

		for _, p := range posts {
			if searchType == "Hashtags" {
				exactMatch := false
				for _, tag := range strings.Split(p.Hashtags, " ") {
					if termMap[strings.ToUpper(tag)] {
						exactMatch = true
					}
				}
				if !exactMatch {
					continue
				}
			}
			list.AddPost(p)
			list.AddOrder(p.Id)
		}

		list.MakeNonNil()

		result.Data = list

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkPostStore) GetForExport(channelId string) StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		/*var posts []*model.Post
		_, err := s.GetReplica().Select(
			&posts,
			"SELECT * FROM Posts WHERE ChannelId = :ChannelId AND DeleteAt = 0",
			map[string]interface{}{"ChannelId": channelId})
		if err != nil {
			result.Err = model.NewLocAppError("RethinkPostStore.GetForExport", "store.rethink_post.get_for_export.app_error", nil, "channelId="+channelId+err.Error())
		} else {
			result.Data = posts
		}*/

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkPostStore) AnalyticsUserCountsWithPostsByDay(teamId string) StoreChannel {
	storeChannel := make(StoreChannel)

	// SELECT t1.Name, COUNT(t1.UserId) AS Value
	// FROM (SELECT DISTINCT
	// 	DATE(FROM_UNIXTIME(Posts.CreateAt / 1000)) AS Name,
	//	    Posts.UserId
	//      FROM Posts, Channels
	//      WHERE Posts.ChannelId = Channels.Id`
	//	AND Channels.TeamId = :TeamId"
	//	AND Posts.CreateAt <= :EndTime
	//	    ORDER BY Name DESC) AS t1
	// GROUP BY Name
	// ORDER BY Name DESC
	// LIMIT 30
	go func() {
		result := StoreResult{}

		/*end := utils.MillisFromTime(utils.EndOfDay(utils.Yesterday()))

		var term rethink.Term
		if len(teamId) > 0 {
			filter := rethink.Row.Field("CreateAt").Le(end).And(rethink.Row.Field("TeamId").Eq(teamId))
			term = rethink.Table("Posts").EqJoin("UserId", rethink.Table("Users")).
				Without(map[string]string{"right": "Id", "right": "CreateAt"}).Zip().
				Filter(filter)
		} else {
			filter := rethink.Row.Field("CreateAt").Le(end)
			term = rethink.Table("Posts").Filter(filter)

		}
		term = term.Map(func(row rethink.Term) {
			rethink.Expr(map[string]interface{}{
				"Name": rethink.EpochTime(row.Field("CreateAt")).Date(),
				"UserId": row.Field("UserId"),
			}).Group("Name").
		}).OrderBy(rethink.Desc("Name")).Limit(30)

		var rows model.AnalyticsRows
		cursor, err := term.Run(s.rethink, runOpts)
		if err != nil {
			result.Err = model.NewLocAppError("RethinkPostStore.AnalyticsUserCountsWithPostsByDay",
				"store.rethink_post.analytics_user_counts_posts_by_day.app_error", nil, err.Error())
		} else if err := cursor.All(&rows); err != nil {
			result.Err = model.NewLocAppError("RethinkPostStore.AnalyticsUserCountsWithPostsByDay",
				"store.rethink_post.analytics_user_counts_posts_by_day.cursor.app_error",
				nil, err.Error())
		} else {
			result.Data = rows
		}*/

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkPostStore) AnalyticsPostCountsByDay(teamId string) StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		/*query :=
			`SELECT
		    Name, COUNT(Value) AS Value
		FROM
		    (SELECT
			DATE(FROM_UNIXTIME(Posts.CreateAt / 1000)) AS Name,
			    '1' AS Value
		    FROM
			Posts, Channels
		    WHERE
			Posts.ChannelId = Channels.Id`

		if len(teamId) > 0 {
			query += " AND Channels.TeamId = :TeamId"
		}

		query += ` AND Posts.CreateAt <= :EndTime
			            AND Posts.CreateAt >= :StartTime) AS t1
			GROUP BY Name
			ORDER BY Name DESC
			LIMIT 30`

		end := utils.MillisFromTime(utils.EndOfDay(utils.Yesterday()))
		start := utils.MillisFromTime(utils.StartOfDay(utils.Yesterday().AddDate(0, 0, -31)))

		var rows model.AnalyticsRows
		_, err := s.GetReplica().Select(
			&rows,
			query,
			map[string]interface{}{"TeamId": teamId, "StartTime": start, "EndTime": end})
		if err != nil {
			result.Err = model.NewLocAppError("RethinkPostStore.AnalyticsPostCountsByDay", "store.rethink_post.analytics_posts_count_by_day.app_error", nil, err.Error())
		} else {
			result.Data = rows
		}*/

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}

func (s RethinkPostStore) AnalyticsPostCount(teamId string, mustHaveFile bool, mustHaveHashtag bool) StoreChannel {
	storeChannel := make(StoreChannel)

	go func() {
		result := StoreResult{}

		/*query :=
			`SELECT
		    COUNT(Posts.Id) AS Value
		FROM
		    Posts,
		    Channels
		WHERE
		    Posts.ChannelId = Channels.Id`

		if len(teamId) > 0 {
			query += " AND Channels.TeamId = :TeamId"
		}

		if mustHaveFile {
			query += " AND Posts.Filenames != '[]'"
		}

		if mustHaveHashtag {
			query += " AND Posts.Hashtags != ''"   858-1900
		}

		if v, err := s.GetReplica().SelectInt(query, map[string]interface{}{"TeamId": teamId}); err != nil {
			result.Err = model.NewLocAppError("RethinkPostStore.AnalyticsPostCount", "store.rethink_post.analytics_posts_count.app_error", nil, err.Error())
		} else {
			result.Data = v
		}*/

		storeChannel <- result
		close(storeChannel)
	}()

	return storeChannel
}
