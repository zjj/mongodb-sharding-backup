import os
import sys
import time
import commands
import threading
from datetime import datetime
from pymongo import MongoClient
from pymongo.collection import ReturnDocument
from pymongo.write_concern import WriteConcern
from pymongo.read_concern import ReadConcern
from pymongo.uri_parser import parse_uri


def stop_balancer():
    pass

def enable_balancer():
    pass

shards = (
    # put the config rs as the first element 
    "mongodb://user:passwd@papapapa/?replicaSet=cfg-rs0",
    "mongodb://user:passwd@papapapa/?replicaSet=mongodb-rs0",
    "mongodb://user:passwd@papapapa/?replicaSet=mongodb-rs1",
)

def Client(user, pwd, host, port=27017):
    url = 'mongodb://%s:%s@%s:%s/'%(user, pwd, host, port)
    return MongoClient(url)

class RS(MongoClient):
    def __init__(self, url, *args, **kwargs):
        super(RS, self).__init__(url, *args, **kwargs)
        parser = parse_uri(url)
        self._user = parser.get('username')
        self._passwd = parser.get('password')
        self.replicaset = parser.get('options').get('replicaset')
        self.secondary_cli = None
        time.sleep(1)
        
    def detach_one_secondary(self):
        addr, port =  self.secondaries.pop()
        self.secondary_cli = Client(self._user, self._passwd, addr, port)
    
    def lock_secondary(self):
        if self.secondary_cli.is_primary:
            print "error, backup can only start on secondary nodes!"
            return False
        try:
            self.secondary_cli.fsync(lock=True)
            if not self.secondary_cli.is_locked:
                print "secondary_cli.is_locked false"
                raise
            else:
                return True
        except Exception as e:
            print e
            try:
                self.secondary_cli.unlock()
            except:
                pass
            return False
        
    def unlock_secondary(self):
        try:
            self.secondary_cli.unlock()
            return True
        except:
            return False

    def backup_rs(self, out):
        if not self.secondary_cli.is_locked:
            return False
        (addr, port) = self.secondary_cli.address
        cmd = ('/usr/bin/mongodump --oplog --gzip '
               '--host %s --port %s --username %s --password %s --out %s'
                )%(addr, port, self._user, self._passwd, out)
        ret = os.system(cmd)
        return not bool(ret)

    def __del__(self):
        if self.secondary_cli:
            self.unlock_secondary()
            self.secondary_cli.close()
        self.close()

class CSRS(RS):
    def configrs_backupcontrol_verify(self):
        writeconcern = WriteConcern(w='majority', wtimeout=15000)
        backupcontrol = self.config.get_collection('BackupControl', write_concern=writeconcern)
        ret = backupcontrol.find_one_and_update({'_id':'BackupControlDocument'},
                                   update={'$inc': {'counter': 1}},
                                   return_document=ReturnDocument.AFTER,
                                   upsert=True)

        cnt_new = ret.get('counter') if ret else 0
        readconcern = ReadConcern("majority")    
        secondary_config = self.secondary_cli.config
        backupcontrol = secondary_config.get_collection('BackupControl', read_concern=readconcern)
        ret = backupcontrol.find_one({ "_id" : "BackupControlDocument"})
        cnt_now = ret.get('counter') if ret else 0

        return cnt_new == cnt_now and cnt_new != 0:

    def lock_secondary(self):
        verified = self.configrs_backupcontrol_verify()
        return super(CSRS, self).lock_secondary() if verified else verified
            

if __name__ == '__main__':
    now = datetime.now().strftime("%Y%m%d")

    all_rs = []

    cfg_rs_url = shards[0]
    rs = CSRS(cfg_rs_url)
    rs.detach_one_secondary()
    rs.lock_secondary()  
    all_rs.append(rs)

    for rs_url in shards[1:]:
        rs = RS(rs_url)
        rs.detach_one_secondary()
        rs.lock_secondary()
        all_rs.append(rs)

    status = ''
    for rs in all_rs:
        if not rs.secondary_cli.is_locked:
            status = 'error'
            break

    if status == 'error':
        for rs in all_rs:
            rs.unlock_secondary()
    else: ##success
        cfg_rs = all_rs[0]
        cfg_rs.backup_rs(os.path.join(now, cfg_rs.replicaset))
        cfg_rs.unlock_secondary()
    
        ## parall
        def do_backup_thread(rs, out):
            rs.backup_rs(out)
            rs.unlock_secondary()

        threads = []
        for rs in all_rs[1:]:
            out = os.path.join(now, rs.replicaset)
            t = threading.Thread(target=do_backup_thread, args=(rs, out))
            t.start()
            threads.append(t)

        for t in threads:
            t.join()
