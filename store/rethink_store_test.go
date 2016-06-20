package store

import "github.com/mattermost/platform/utils"

func SetupRethink() {
	if store == nil {
		utils.LoadConfig("config.json")
		utils.InitTranslations(utils.Cfg.LocalizationSettings)
		utils.ConfigureCmdLineLog()
		store = NewRethinkStore()

		store.MarkSystemRanUnitTests()
		store.(*RethinkStore).WatchTransactions()
	}
	// This actually truncates, not sure why they called it DropAllTables()
	//store.DropAllTables()
}
