this is a script for backup(dump) mongo sharded cluster
<pre>
mongo --host MONGOS --port 27017 << EOF
use admin;
db.auth('root', 'XXXXXXXXX');
use config;
sh.stopBalancer();
EOF

python backup.py

mongo --host MONGOS --port 27017 << EOF
use admin;
db.auth('root', 'XXXXXXXXX');
use config;
sh.setBalancerState(true);
EOF
</pre>
