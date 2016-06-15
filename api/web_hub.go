// Copyright (c) 2015 Mattermost, Inc. All Rights Reserved.
// See License.txt for license information.

package api

import (
	l4g "github.com/alecthomas/log4go"
	rethink "github.com/dancannon/gorethink"
	"github.com/mattermost/platform/model"
	"github.com/mattermost/platform/utils"
)

type Hub struct {
	connections       map[*WebConn]bool
	register          chan *WebConn
	unregister        chan *WebConn
	broadcast         chan *model.Message
	stop              chan string
	invalidateUser    chan string
	invalidateChannel chan string
	rethink           *rethink.Session
}

var hub = &Hub{
	register:          make(chan *WebConn),
	unregister:        make(chan *WebConn),
	connections:       make(map[*WebConn]bool),
	broadcast:         make(chan *model.Message),
	stop:              make(chan string),
	invalidateUser:    make(chan string),
	invalidateChannel: make(chan string),
	rethink:           nil,
}

// TODO: This should return an error if we are unable to put the message in the hub table
// TODO: Or should this queue up the messages until the hub table has been restored?
func Publish(message *model.Message) {
	// TODO: Thrawn - This should send the message to rethinkdb instead of placing the message on the
	// TODO: broadcast channel. Messages should arrive on the broadcast channel via rethinkdb event listeners
	l4g.Debug("api.web_hub.PublishAndForget.rethinkdb.insert %s", message.ToJson())
	_, err := rethink.DB(utils.Cfg.RethinkSettings.Database).
		Table(utils.Cfg.RethinkSettings.HubTable).Insert(message).RunWrite(hub.rethink)
	if err != nil {
		l4g.Error("api.web_hub.PublishAndForget.rethinkdb %s", err.Error())
	}
}

func InvalidateCacheForUser(userId string) {
	hub.invalidateUser <- userId
}

func InvalidateCacheForChannel(channelId string) {
	hub.invalidateChannel <- channelId
}

func (h *Hub) Register(webConn *WebConn) {
	h.register <- webConn
}

func (h *Hub) Unregister(webConn *WebConn) {
	h.unregister <- webConn
}

func (h *Hub) Broadcast(message *model.Message) {
	if message != nil {
		h.broadcast <- message
	}
}

func (h *Hub) Stop() {
	h.stop <- "all"
}

func (h *Hub) Start() {
	go func() {
		for {
			select {
			case webCon := <-h.register:
				h.connections[webCon] = true

			case webCon := <-h.unregister:
				if _, ok := h.connections[webCon]; ok {
					delete(h.connections, webCon)
					close(webCon.Send)
				}
			case userId := <-h.invalidateUser:
				for webCon := range h.connections {
					if webCon.UserId == userId {
						webCon.InvalidateCache()
					}
				}

			case channelId := <-h.invalidateChannel:
				for webCon := range h.connections {
					webCon.InvalidateCacheForChannel(channelId)
				}
			// TODO: Thrawn - Broadcast events should come from a go routine that watches for events from
			// TODO: rethinkdb. This should decouple the hub from any central server
			case msg := <-h.broadcast:
				for webCon := range h.connections {
					if shouldSendEvent(webCon, msg) {
						select {
						case webCon.Send <- msg:
						default:
							close(webCon.Send)
							delete(h.connections, webCon)
						}
					}
				}

			case s := <-h.stop:
				l4g.Debug(utils.T("api.web_hub.start.stopping.debug"), s)

				for webCon := range h.connections {
					webCon.WebSocket.Close()
				}

				return
			}
		}
	}()
}

func shouldSendEvent(webCon *WebConn, msg *model.Message) bool {

	if webCon.UserId == msg.UserId {
		// Don't need to tell the user they are typing
		if msg.Action == model.ACTION_TYPING {
			return false
		}

		// We have to make sure the user is in the channel. Otherwise system messages that
		// post about users in channels they are not in trigger warnings.
		if len(msg.ChannelId) > 0 {
			allowed := webCon.HasPermissionsToChannel(msg.ChannelId)

			if !allowed {
				return false
			}
		}
	} else {
		// Don't share a user's view or preference events with other users
		if msg.Action == model.ACTION_CHANNEL_VIEWED {
			return false
		} else if msg.Action == model.ACTION_PREFERENCE_CHANGED {
			return false
		} else if msg.Action == model.ACTION_EPHEMERAL_MESSAGE {
			// For now, ephemeral messages are sent directly to individual users
			return false
		}

		// Only report events to users who are in the team for the event
		if len(msg.TeamId) > 0 {
			allowed := webCon.HasPermissionsToTeam(msg.TeamId)

			if !allowed {
				return false
			}
		}

		// Only report events to users who are in the channel for the event execept deleted events
		if len(msg.ChannelId) > 0 && msg.Action != model.ACTION_CHANNEL_DELETED {
			allowed := webCon.HasPermissionsToChannel(msg.ChannelId)

			if !allowed {
				return false
			}
		}
	}

	return true
}
