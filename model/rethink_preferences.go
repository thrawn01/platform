package model

import "encoding/json"

type RethinkPreferences struct {
	UserId      string       `json:"user_id"`
	Preferences []Preference `json:"preferences"`
}

func (o RethinkPreferences) ToJson() string {
	b, err := json.Marshal(o)
	if err != nil {
		return ""
	} else {
		return string(b)
	}
}

func (o RethinkPreferences) GetPreference(name, category string) (Preference, bool) {
	for _, value := range o.Preferences {
		if value.Name == name && value.Category == category {
			return value, true
		}
	}
	return Preference{}, false
}

func (o RethinkPreferences) GetCategories(category string) Preferences {
	results := Preferences{}
	for idx, value := range o.Preferences {
		if value.Category == category {
			results = append(results, o.Preferences[idx])
		}
	}
	return results
}

func RethinkPreferencesFromPreferences(prefs *Preferences) RethinkPreferences {
	result := RethinkPreferences{}
	if len(*prefs) == 0 {
		return result
	}
	for _, value := range *prefs {
		result.Preferences = append(result.Preferences, value)
	}
	result.UserId = (*prefs)[0].UserId
	return result
}

func PreferencesFromRethinkPreferences(pref RethinkPreferences) Preferences {
	result := Preferences{}
	if len(pref.Preferences) == 0 {
		return result
	}
	for _, value := range pref.Preferences {
		result = append(result, value)
	}
	return result
}
