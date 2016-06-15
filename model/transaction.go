package model

import "encoding/json"

type Transaction struct {
	Id       string
	Type     string
	State    string
	ErrorStr string
	Model    interface{}
}

func (self *Transaction) ToJson() string {
	b, err := json.Marshal(self)
	if err != nil {
		return ""
	} else {
		return string(b)
	}
}

func (self *Transaction) PreSave() {
	if self.Id == "" {
		self.Id = NewId()
	}
}

type DirectChannelTransaction struct {
	Channel *Channel
	Members []*ChannelMember
}
