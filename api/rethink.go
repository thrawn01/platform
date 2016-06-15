package api

import (
	"strings"

	l4g "github.com/alecthomas/log4go"
	rethink "github.com/dancannon/gorethink"
	"github.com/mattermost/platform/model"
	"github.com/mattermost/platform/store"
	"github.com/mattermost/platform/utils"
)

func (me *TestHelper) InitRethink() *TestHelper {
	session, err := rethink.Connect(rethink.ConnectOpts{
		Addresses: utils.Cfg.RethinkSettings.Addresses,
		Username:  utils.Cfg.RethinkSettings.Username,
		Password:  utils.Cfg.RethinkSettings.Password,
	})
	if err != nil {
		l4g.Error("Database connection failed:", err)
		panic(err)
	}
	hub.rethink = session

	me.CreateRethinkTables()

	WatchHubs()

	return me
}

func (me *TestHelper) CreateRethinkTables() *TestHelper {
	// Create the required tables
	response, err := rethink.DB(utils.Cfg.RethinkSettings.Database).
		TableCreate(utils.Cfg.RethinkSettings.HubTable).RunWrite(hub.rethink)
	if err != nil {
		l4g.Error("api.rethink.CreateRethinkTables.create %s", err.Error())
		if !strings.Contains(err.Error(), "already exists") {
			panic(err.Error())
		}
	}
	l4g.Debug("%d Tables Created", response.TablesCreated)
	return me
}


func WatchHubs() {
	go func() {
		// Watch for changes on the 'hub' table
		hubQuery, err := rethink.DB(utils.Cfg.RethinkSettings.Database).
			Table(utils.Cfg.RethinkSettings.HubTable).Changes().Run(hub.rethink)
		if err != nil {
			l4g.Error("api.rethink.WatchHubs.watch-hub-table %s", err.Error())
			panic(err.Error())
		}

		var change map[string]*model.Message
		for hubQuery.Next(&change) {
			// When changes on the hub table come in, broadcast them to our connected clients
			l4g.Debug("api.rethink.WatchHubs.broadcast %s", change["new_val"].ToJson())
			hub.Broadcast(change["new_val"])
		}
	}()
}
