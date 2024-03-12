# Point of Sale

A [PeterOS](https://github.com/Platratio34/peterOS) [pgm-get](https://github.com/peterOS-pgm-get/pgm-get) program

Install on PeterOS via:
```console
pgm-get install netdb
```

## Command

### `netdb`: CLI DB access

### `netdb using [database]`: CLI DB access: Using
Select a local database to interact with for `netdb` commands

### `netdb list <tables>`: CLI DB access: List
Lists all databases or tables in the current selected database

### `netdb create <database|table> [name]`: CLI DB access: Create
Create a datable or table in current database

### `netdb remote [server] [database] <[user] [password]>`: CLI Remote DB access
Start a DB session with a remote database.

If remote database is user controlled `user` and `password` must be provided

### `netdb host`: NetDB Server start
Hosts a NetDB server on this machine.

Equivalent to running `netdb.server.start()`


## Program package: `_G.netdb`
[Documentation](https://github.com/peterOS-pgm-get/netdb/wiki)