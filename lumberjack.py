import threading
import hashlib
import collections
import sys

from http import Service
from uuid import uuid4 as uuid


def LOG(*args):
    for a in args:
        print >> sys.stderr, a,
    print >> sys.stderr

_message = collections.namedtuple('Message','mid mh md5 body attrs') 

def message(body):
    return _message(uuid(), uuid(), hashlib.md5(body).hexdigest(), body, None)

message.attributes = ['SenderId', 'SentTimestamp', 'ApproximateReceiveCount','ApproximateFirstReceiveTimestamp']
 
class MessageQueue(object):
    def __init__(self, vistimeout):
        self.inbox = []
        self.vistimeout = vistimeout

    def send(self, msg):
        self.inbox.append(msg)
        return msg
    
    def delete(self, handle):
        for i,m in enumerate(self.inbox):
            if m.mh == handle:
                del self.inbox[i]
                break

    def read(self, num, vistimeout):
        vis = vistimeout or self.vistimeout
        return self.inbox[:num]

    def attribute(self, name):
        if name == "ApproximateNumberOfMessages":
            return len(self.inbox)

class QueueService(object):
    def __init__(self):
        self.queues = {}

    def all_queues(self, prefix):
        return self.queues.keys()
 
    def send(self, queue, body):
        LOG('SEND', queue)
        return self.queues[queue].send(message(body))

    def create(self, queue, vis):
        LOG('CREATE', queue)
        qid = '/'+queue
        if not qid in self.queues:
            self.queues[qid] = MessageQueue(vis)
        return qid

    def read(self, queue, max_num, visibilitytimeout, attributes):
        LOG('READ', queue)
        qid = '/'+queue
        return self.queues[queue].read(max_num, visibilitytimeout)

    def delete_msg(self, queue, handle):
        LOG('DELMSG', queue)
        self.queues[queue].delete(handle)

    def delete_queue(self, queue):
        LOG('DELETE', queue)
        del self.queues[queue]

    def attribute(self, queue, name):
        return self.queues[queue].attribute(name)

class TestService(Service):
    def __init__(self, queue):
        self.queue = queue
        self.lock = threading.Lock()

    def handle(self, root, request):
        data = request.data if request.data else request.query
        action = str(data.get('Action', 'default'))
        try:
            ret = getattr(self, action)(root, request.path, data)
            return ret
        except:
            import traceback
            traceback.print_exc()
            raise

    def ListQueues(self, root, path, params):
        prefix = params.get('QueueNamePrefix', '')

        queues = []
        with self.lock:
            for q in  self.queue.all_queues('/'+prefix):
                queues.append("<QueueUrl>"+root+q[1:]+"</QueueUrl>")
        return  """
            <ListQueuesResponse>
                <ListQueuesResult>
                    %s    
                </ListQueuesResult>
                <ResponseMetadata>
                    <RequestId>
                        %s
                    </RequestId>
                </ResponseMetadata>
            </ListQueuesResponse>
        """%("\n".join(queues), uuid())


    def CreateQueue(self, root, path, params):
        name = params['QueueName']
        visible = int(params.get('DefaultVisibilityTimeout', 30))
        with self.lock:
            qid = self.queue.create(name, visible)
        return """
            <CreateQueueResponse>
                <CreateQueueResult>
                    <QueueUrl>%s%s</QueueUrl>
                </CreateQueueResult>
                <ResponseMetadata>
                    <RequestId>%s</RequestId>
                </ResponseMetadata>
            </CreateQueueResponse>
        """%(root,qid[1:], uuid())      

    def SendMessage(self, root, path, params):
        body = params['MessageBody']
        with self.lock:
            msg = self.queue.send(path, body)
        return """<SendMessageResponse>
                <SendMessageResult>
                    <MD5OfMessageBody>%s</MD5OfMessageBody>
                    <MessageId>%s</MessageId>
                </SendMessageResult>
                <ResponseMetadata>
                    <RequestId>%s</RequestId>
                </ResponseMetadata>
            </SendMessageResponse>
        """%(msg.md5, msg.mid, uuid())


    def ReceiveMessage(self, root, path, params):
        attributename = params.get('AttributeName','')
        max_msg = int(params.get('MaxNumberOfMessages', 1))
        visible = params.get('VisibilityTimeout', None)
        visible = int(visible) if visible else None

        if attributename == 'All':
            attributes = message.attributes
        elif attributename in message.attributes:
            attributes = (attributename,)
        else:
            attributes = ()
            
        with self.lock:
            msgs = self.queue.read(path, max_msg, visible, attributes)

        messages = []
        for m in msgs:
            mid, mh, md5, body, attrs = m
            attr_xml = []
            if attrs:
                for k,v in attrs.iteritems():
                    attr_xml.append("""
                      <Attribute>
                        <Name>%s</Name>
                        <Value>%s</Value>
                      </Attribute>
                    """)
            messages.append("""
             <Message>
                  <MessageId>%s</MessageId>
                  <ReceiptHandle>%s</ReceiptHandle>
                  <MD5OfBody>%s</MD5OfBody>
                  <Body>%s</Body>
                  %s
            </Message>
            """%(mid, mh, md5, body, "".join(attr_xml)))

        return """
            <ReceiveMessageResponse>
              <ReceiveMessageResult>%s</ReceiveMessageResult>
              <ResponseMetadata>
                <RequestId>%s</RequestId>
              </ResponseMetadata>
            </ReceiveMessageResponse>
        """%("\n".join(messages), uuid())

    def DeleteMessage(self, root, path, request):
        handle = request['ReceiptHandle']

        with self.lock:
            self.queue.delete_msg(path, handle)

        return """
            <DeleteMessageResponse>
                <ResponseMetadata>
                    <RequestId>%s</RequestId>
                </ResponseMetadata>
            </DeleteMessageResponse>
        """%(uuid()) 

    def DeleteQueue(self, root, path, request):

        with self.lock:
            self.queue.delete_queue(path)

        return """
            <DeleteQueueResponse>
                <ResponseMetadata>
                    <RequestId>%s</RequestId>
                </ResponseMetadata>
            </DeleteQueueResponse>
        """%(uuid())


    def GetQueueAttributes(self, root, path, request):
        attr = str(request['AttributeName'])
        with self.lock:
            val = str(self.queue.attribute(path, attr))
        return """
            <GetQueueAttributesResponse>
              <GetQueueAttributesResult>
                <Attribute>
                  <Name>%s</Name>
                  <Value>%s</Value>
                </Attribute>
              </GetQueueAttributesResult>
              <ResponseMetadata>
                <RequestId>%s</RequestId>
              </ResponseMetadata>
            </GetQueueAttributesResponse>
        """%(attr,val, uuid())
            

    def default(self, root, path, request):
        raise StandardError('unknown')

if __name__ == '__main__':
    server = TestService(QueueService()).run(port=2222, block=False, ssl=False)
    print server.get_url()
    server.join()
    


