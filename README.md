Truelist websockets server. Forwards messages from the Redis pub/sub channel
`clearlist_realtime` to browser sockets registered on each message's channel.

See `CLAUDE.md` for the message protocol, env vars, tests and image tags.

Env vars:
* `DEBUG`: if set, will output more verbose logging statements
* `PORT`: the port to run on.  defaults to `8080`
* `REDIS_URL`: the url that redis is on
* `ALLOWED_ORIGINS`: comma-separated browser origins allowed to connect
* `ALLOW_DEV_ORIGINS`: if `true`, also allow staging and localhost origins
