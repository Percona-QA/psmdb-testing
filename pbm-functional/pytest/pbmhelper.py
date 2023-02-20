import testinfra
import time
import json
import os

TIMEOUT = int(os.getenv("TIMEOUT",default = 300))

def setup_pbm(node):
    n = testinfra.get_host("docker://" + node)
    result = n.check_output('pbm config --file=/etc/pbm.conf --out=json')
    print(json.loads(result))
    time.sleep(10)

def find_event_msg(node,event,msg):
    n = testinfra.get_host("docker://" + node)    
    command = "pbm logs --tail=0 --out=json --event=" + event
    logs = n.check_output(command)
    for log in json.loads(logs):
        if log['msg'] == msg:
             return log
             break

def find_backup(node,name):
    n = testinfra.get_host("docker://" + node)
    list = n.check_output('pbm list --out=json')
    parsed_list = json.loads(list)
    if parsed_list['snapshots']:
        for snapshot in parsed_list['snapshots']:
            if snapshot['name'] == name:
                return snapshot
                break

def get_pbm_logs(node):
    n = testinfra.get_host("docker://" + node)
    logs = n.check_output("pbm logs -s D --tail=0")
    print(logs)

def check_status(node):
    n = testinfra.get_host("docker://" + node)
    status = n.check_output('pbm status --out=json')
    running = json.loads(status)['running']
    if running:
        return running

def check_pitr(node):
    n = testinfra.get_host("docker://" + node)
    status = n.check_output('pbm status --out=json')
    running = json.loads(status)['pitr']['run']
    return bool(running)

def check_agents_status(node):
    n = testinfra.get_host("docker://" + node)
    result = n.check_output('pbm status --out=json')
    parsed_result = json.loads(result)
    for replicaset in parsed_result['cluster']:
        for host in replicaset['nodes']:
            assert host['ok'] == True

def check_mongod_service(node):
    n = testinfra.get_host("docker://" + node)
    service = n.service("mongod")
    assert service.is_running

def check_pbm_service(node):
    n = testinfra.get_host("docker://" + node)
    service = n.service("pbm-agent")
    assert service.is_running


def make_backup(node,type):
    n = testinfra.get_host("docker://" + node)
    for i in range(TIMEOUT):
        running = check_status(node)
        if not running:
            if type:
                start = n.check_output('pbm backup --out=json --type=' + type )
            else:
                start = n.check_output('pbm backup --out=json')
            name = json.loads(start)['name']
            print("backup started:")
            print(name)
            break
        else:
            print("unable to start backup - another operation in work")
            print(running)
            time.sleep(1)
    for i in range(TIMEOUT):
        running = check_status(node)
        print("current operation:")
        print(running)
        result = find_backup(node,name)
        if result:
            print("backup found:")
            print(result)
            assert result['status'] == 'done'
            return name
            break
        else:
            time.sleep(1)

def make_restore(node,name):
    n = testinfra.get_host("docker://" + node)
    for i in range(TIMEOUT):
        running = check_status(node)
        if not running:
            output = n.check_output('pbm restore ' + name + ' --wait')
            print(output)
            break
        else:
            print("unable to start restore - another operation in work")
            print(running)
            time.sleep(1)

def make_resync(node):
    n = testinfra.get_host("docker://" + node)
    output = n.check_output('pbm config --force-resync')
    print(output)
    for i in range(TIMEOUT):
        logs = find_event_msg(node,"resync","succeed")
        if logs:
            print(logs)
            break
        else:
            time.sleep(1)
    time.sleep(5)

def restart_pbm_agents(nodes):
    for node in nodes:
        n = testinfra.get_host("docker://" + node)
        n.check_output('supervisorctl restart pbm-agent')

