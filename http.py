import sys
import os
import cgi
import urllib
import urlparse
import collections
import threading
import StringIO
import socket
from SocketServer import BaseServer
from OpenSSL import SSL

# assuming 2.6.x

from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler

from SocketServer import ThreadingMixIn

filename = lambda x: os.path.join(os.path.split(__file__)[0],x)

key_file = filename("server.key")
cert_file = filename("server.pem")

class HTTPServer(ThreadingMixIn,HTTPServer):
    pass

class SSLServer(ThreadingMixIn,HTTPServer):
    def __init__(self, server_address, HandlerClass):
        BaseServer.__init__(self, server_address, HandlerClass)
        ctx = SSL.Context(SSL.SSLv23_METHOD)
        ctx.use_privatekey_file(key_file)
        ctx.use_certificate_file(cert_file)
        ctx.set_cipher_list('HIGH')
        self.socket = SSL.Connection(ctx, socket.socket(self.address_family, self.socket_type))
        self.server_bind()
        self.server_activate()


Request=collections.namedtuple('Request', 'method path query data headers')


class Service(object):
    def run(self, port=0, block=True, ssl=False):
        server = WebServer(self, port=port, ssl=ssl)
        server.start()
        if block:
            try:
                server.join()
            except KeyboardInterrupt:
                sys.exit(100)
        return server

    def close(self):
        pass

    def get_url(self, p):
        return self.server.get_url


class Handler(BaseHTTPRequestHandler):
    def setup(self):
        self.connection = self.request
        self.rfile = socket._fileobject(self.request, "rb", self.rbufsize)
        self.wfile = socket._fileobject(self.request, "wb", self.wbufsize)
    
    def log_message(*a,**b):
        pass

    def do_GET(self):
        self.do_method("GET")
    def do_POST(self):
        self.do_method("POST")

    def do_PUT(self):
        self.do_method("PUT")
    def do_DELETE(self):
        self.do_method("DELETE")

    def do_method(self, method):
        try:
            url = urlparse.urlparse(self.path)
            path = url.path
            query = {}
            for k,v in cgi.parse_qs(url.query).iteritems():
                query[k] = v[0]
            length = 0

            if 'Content-Length' in self.headers:
                length = int(self.headers['Content-length'])

            if length:
                post=self.rfile.read(length)
                data = {}
                for k,v in cgi.parse_qs(post).iteritems():
                    data[k] = v[0]
            else:
                post=""
                data=None

            r = Request(method=method, path=path, query=query, data=data, headers=self.headers)

            out = "err"
            try:
                out = str(self.on_request(r))
            except:
                self.send_response(500)
                self.send_header('Content-type', 'text/plain')
                import traceback
                out = str(traceback.format_exc())            
            else:
                self.send_response(200)
                self.send_header('Content-type', 'text/xml')

            finally:
                self.send_header('Content-Length', len(out))
                self.end_headers()
                self.wfile.write(out)

            return
        except:
            import traceback
            traceback.print_exc()
            raise

    def on_request(self,request):
        return ""



class WebServer(threading.Thread):
    
    def __init__(self,app,host="localhost",port=0, ssl=False):
        threading.Thread.__init__(self)
        self.app=app
        self.port=int(port)
        self.host=host
        self.running = False
        self._init_complete = threading.Event()
        self.ssl=ssl

    def get_url(self, path="/"):
        if not path.startswith("/"):
            path = "/"+path
        return "http%s://%s:%s%s"%(("s" if self.ssl else ""), self.server.server_name, self.server.server_port, path)


    def run(self):
        self.running = True
        http_server = self

        class requesthandler(Handler):
            def on_request(handler, request):
                return self.app.handle(self.get_url(), request)


        if self.ssl:
            self.server = SSLServer((self.host, self.port),requesthandler)
        else:
            self.server = HTTPServer((self.host, self.port),requesthandler)

        self._init_complete.set()
        try:
            while self.running:
                try:
                    self.server.handle_request()
                except (StopIteration, GeneratorExit, KeyboardInterrupt, SystemExit) as e:
                    raise
                except:
                    import traceback
                    traceback.print_exc()
                    pass

        finally:
            import traceback
            traceback.print_exc()
            
    def stop(self):
        self.running=False
        # server blocks on incoming connections, so
        # we make a connection to get it to shut down
        # It needs to do one connection before checking self.running
        try:
            # 
            urllib.urlopen(self.get_url("/"))
        except IOError:
            pass
        self.app.close()

    def start(self):
        threading.Thread.start(self)
        while not self._init_complete.isSet():
            self._init_complete.wait(2)

    
class TestService(Service):
    def handle(self, server, request):
        r = ["<xml>"]
        for k,v in request.query:
            r.append("<%s>%s</%s>"%(k,v,k))
        r.append("</xml>")
        return "\n".join(r)

if __name__ == '__main__':
    TestService().run(port=2202)


