#! /usr/bin/python

import rethinkdb as r

# This needs to go into the rethinkdb container and run on startup, we should
# store the username and password in kubernetes security and fetch it for this
# script.

conn = r.connect('dockerhost', 28015)

# Add our users
r.db('rethinkdb').table('users').insert({'id': 'mmuser', 'password': 'mmpass'})

# Create the database
r.db_create('mattermost');

# Grant the ability to read and write tables
r.db('mattermost').grant('mmuser', {'read': True, 'write': True, 'config': True});

# The application is responsible for creating the tables it needs... not sure
# if this is the secure way todo it, or just the most simple.

