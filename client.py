
import boto
import sys
boto.config.setbool('Boto','https_validate_certificates', False)


from boto.sqs.connection import SQSConnection
from boto.sqs.regioninfo import SQSRegionInfo

from boto.sqs.message import Message

def sqs_connection(name, hostname, port, ssl=False):
    region = SQSRegionInfo(name=name, endpoint=hostname)

    return SQSConnection('key_id', 'key', region=region, port=port, debug=0, is_secure=ssl, path="/")


if __name__ == '__main__':
    conn = sqs_connection('test', '127.0.0.1', 2222)
    q = conn.create_queue('myqueue')
    print 'created', q
    print 'queues',conn.get_all_queues()
    m = Message()
    m.set_body('toot')

    print q.write(m)

    q2 = conn.get_all_queues()[0]
    print q2.count()

    rs = q2.get_messages(1, 60)

    for r in rs:
        print 'got',r.get_body()

        q.delete_message(r)

    conn.delete_queue(q)
