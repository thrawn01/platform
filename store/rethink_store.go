package store

import (
	"fmt"
	"strings"
	"time"

	l4g "github.com/alecthomas/log4go"
	rethink "github.com/dancannon/gorethink"
	"github.com/mattermost/platform/model"
	"github.com/mattermost/platform/utils"
)

var runOpts = rethink.RunOpts{
	Durability: "hard",
}

var runOptsHard = rethink.RunOpts{
	Durability: "hard",
}

var execOpts = rethink.ExecOpts{
	Durability: "hard",
}

type RethinkStore struct {
	rethink       *rethink.Session
	team          *RethinkTeamStore
	channel       *RethinkChannelStore
	user          *RethinkUserStore
	audit         *RethinkAuditStore
	post          PostStore
	compliance    ComplianceStore
	session       SessionStore
	oauth         OAuthStore
	system        SystemStore
	webhook       WebhookStore
	command       CommandStore
	preference    PreferenceStore
	license       LicenseStore
	recovery      PasswordRecoveryStore
	SchemaVersion string
}

func NewRethinkStore() Store {
	store := &RethinkStore{}

	// Loop until we can connect to the rethink
	for {
		session, err := rethink.Connect(rethink.ConnectOpts{
			Addresses: utils.Cfg.RethinkSettings.Addresses,
			Database:  utils.Cfg.RethinkSettings.Database,
			Username:  utils.Cfg.RethinkSettings.Username,
			Password:  utils.Cfg.RethinkSettings.Password,
		})
		if err != nil {
			l4g.Critical("store.rethink.open_conn.critical", err)
			time.Sleep(time.Second)
			continue
		}
		store.rethink = session
		break
	}

	// Create tables common to all stores
	store.CreateTablesIfNotExist()

	// TODO: Need to init the hub somehow (Perhaps move the hub code into the store package?
	// hub.rethink = session

	store.team = NewRethinkTeamStore(store.rethink)
	store.channel = NewRethinkChannelStore(store.rethink)
	store.user = NewRethinkUserStore(store.rethink)
	store.audit = NewRethinkAuditStore(store.rethink)

	store.WatchTransactions()

	return store
}

var isLeader bool
var watchTransactionsRunning bool

func (self RethinkStore) WatchTransactions() {

	// Only allow 1 watch to run
	if watchTransactionsRunning {
		return
	}

	go func() {
		var session *rethink.Session
		var err error

		watchTransactionsRunning = true
		defer func() { watchTransactionsRunning = false }()

		for {
			session, err = rethink.Connect(rethink.ConnectOpts{
				Addresses: utils.Cfg.RethinkSettings.Addresses,
				Database:  utils.Cfg.RethinkSettings.Database,
				Username:  utils.Cfg.RethinkSettings.Username,
				Password:  utils.Cfg.RethinkSettings.Password,
			})
			if err != nil {
				l4g.Critical("store.rethink.watch_transactions.open_conn.critical", err)
				backOff(false)
				continue
			}
			backOff(true)
			break
		}

		// Watch for changes on the 'Transactions' table
		hubQuery, err := rethink.Table("Transactions").Changes().Run(session)
		if err != nil {
			l4g.Error("api.rethink.WatchTransactions.watch-transactions-table %s", err.Error())
			panic(err.Error())
		}

		// TODO: Use etcd to decide who is the WatchTransaction leader
		isLeader = true
		// TODO: If/When we become leader, query the table for un-finished transactions and complete them

		var change map[string]*model.Transaction
		for hubQuery.Next(&change) {
			l4g.Debug("api.rethink.WatchTransactions.change %s", change["new_val"].ToJson())
			// If the transaction is new
			if change["old_val"] == nil {
				// If it's a DirectChannel Transaction
				if change["new_val"].Type == "DirectChannel" {
					self.runTransaction(change["new_val"], self.channel.HandleDirectChannelTransaction)
				}

			}
		}
		session.Close()
	}()
}

func (self RethinkStore) CreateTablesIfNotExist() {
	err := rethink.TableCreate("Transactions", rethink.TableCreateOpts{PrimaryKey: "Id"}).Exec(self.rethink, execOpts)
	handleCreateError("Transactions.CreateTablesIfNotExists().", err)
}

func (self RethinkStore) runTransaction(change *model.Transaction, transFunc func(change *model.Transaction, updateState func(*model.Transaction) error) error) {
	update := func(trans *model.Transaction) error {
		update, err := rethink.Table("Transactions").Get(trans.Id).
			Update(change).RunWrite(self.rethink, runOpts)
		if err != nil {
			return err
		}
		if update.Skipped != 0 {
			return model.NewLocAppError("RethinkStore.WatchTransactions",
				"update.notfound", nil, "id="+trans.Id+"state="+trans.State)
		}
		return nil
	}

	delTransaction := func(id string) error {
		update, err := rethink.Table("Transactions").Get(id).Delete().RunWrite(self.rethink, runOpts)
		if err != nil {
			return err
		}
		if update.Skipped != 0 {
			return model.NewLocAppError("RethinkStore.WatchTransactions", "delTransaction.notfound", nil, "id="+id)
		}
		return nil
	}

	go func() {
		for {
			if isLeader {
				if err := transFunc(change, update); err != nil {
					// There is an error we need to log
					if change.ErrorStr != "" {
						return
					}
					l4g.Error(err.Error())
					backOff(false)
				}
				backOff(true)
				delTransaction(change.Id)
				return
			}
		}
	}()
}

func (self RethinkStore) Close() {
	l4g.Info(utils.T("store.sql.closing.info"))
	if self.rethink != nil {
		self.rethink.Close()
	}
}

func (self RethinkStore) Team() TeamStore {
	return self.team
}

func (self RethinkStore) Channel() ChannelStore {
	return self.channel
}

func (self RethinkStore) Post() PostStore {
	return self.post
}

func (self RethinkStore) User() UserStore {
	return self.user
}

func (self RethinkStore) Session() SessionStore {
	return self.session
}

func (self RethinkStore) Audit() AuditStore {
	return self.audit
}

func (self RethinkStore) Compliance() ComplianceStore {
	return self.compliance
}

func (self RethinkStore) OAuth() OAuthStore {
	return self.oauth
}

func (self RethinkStore) System() SystemStore {
	return self.system
}

func (self RethinkStore) Webhook() WebhookStore {
	return self.webhook
}

func (self RethinkStore) Command() CommandStore {
	return self.command
}

func (self RethinkStore) Preference() PreferenceStore {
	return self.preference
}

func (self RethinkStore) License() LicenseStore {
	return self.license
}

func (self RethinkStore) PasswordRecovery() PasswordRecoveryStore {
	return self.recovery
}

func (self RethinkStore) DropAllTables() {
	cursor, err := rethink.TableList().Run(self.rethink)
	if err != nil {
		l4g.Info("store.sql.DropAllTables.TableList.error", err)
		return
	}

	var tables []string
	if err := cursor.All(&tables); err != nil {
		l4g.Info("store.sql.DropAllTables.TableList.cursor.error", err)
		return
	}

	for _, table := range tables {
		if err := rethink.Table(table).Delete().Exec(self.rethink); err != nil {
			l4g.Info("store.sql.DropAllTables.droptable.error", err)
		}
	}
}

func (self RethinkStore) MarkSystemRanUnitTests() {
	// Not Implemented
	return
}

func handleCreateError(context string, err error) {
	if err != nil {
		if !strings.Contains(err.Error(), "already exists") {
			l4g.Critical(fmt.Sprintf("api.rethink.%s.Create.error", context), err)
			l4g.Close()
			panic(context + " failed")
		}
		l4g.Debug(fmt.Sprintf("api.rethink.%s.Create.alreadyExists %s", context))
	}
}

var dLock = DistributedLock{}

type DistributedLock struct {
}

// Block until the lock is acquired
func (self DistributedLock) Lock(item string) error {
	// Not Implemented
	return nil
}

func (self DistributedLock) UnLock(item string) {
	// Not Implemented
}

var backoffAttempts float32

func backOff(reset bool) {
	if reset {
		backoffAttempts = 0
		return
	}
	if backoffAttempts < 1.6 {
		backoffAttempts += 0.2
	}
	time.Sleep(time.Second * time.Duration(backoffAttempts))
}
