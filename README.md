## Kine (Kine is not etcd)

Kine is an etcdshim that translates etcd API to sqlite and dqlite

### Features
- Can be run standalone so any k8s (not just k3s) can use Kine
- Implements a subset of etcdAPI (not usable at all for general purpose etcd)
- Translates etcdTX calls into the desired API (Create, Update, Delete)
- Provides backend drivers for dqlite, sqlite
