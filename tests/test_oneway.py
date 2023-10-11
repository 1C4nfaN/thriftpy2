import multiprocessing
import thriftpy2
import time
from thriftpy2.rpc import make_client, make_server
from thriftpy2.protocol import TCompactProtocolFactory, TBinaryProtocolFactory
from thriftpy2.transport import TBufferedTransportFactory, TFramedTransportFactory


class Dispatcher(object):
    def Test(self, req):
        print("Get req msg: %s" % req)


oneway_thrift = thriftpy2.load("oneway.thrift", module_name="oneway_thrift")
multiprocessing.set_start_method("fork")


class TestBinaryBuffered(object):
    def setup_class(self):
        server = make_server(oneway_thrift.echo, Dispatcher(), '127.0.0.1', 6000,
                             proto_factory=TBinaryProtocolFactory(), trans_factory=TBufferedTransportFactory())
        self.p = multiprocessing.Process(target=server.serve)
        self.p.start()
        time.sleep(1)  # Wait a second for server to start.

    def teardown_class(self):
        self.p.terminate()

    def test_echo(self):
        req = "Hello!"
        client = make_client(oneway_thrift.echo, '127.0.0.1', 6000,
                             proto_factory=TBinaryProtocolFactory(), trans_factory=TBufferedTransportFactory())

        assert client.Test(req) == None


class TestCompactBuffered(object):
    def setup_class(self):
        server = make_server(oneway_thrift.echo, Dispatcher(), '127.0.0.1', 6000,
                             proto_factory=TCompactProtocolFactory(), trans_factory=TBufferedTransportFactory())
        self.p = multiprocessing.Process(target=server.serve)
        self.p.start()
        time.sleep(1)  # Wait a second for server to start.

    def teardown_class(self):
        self.p.terminate()

    def test_echo(self):
        req = "Hello!"
        client = make_client(oneway_thrift.echo, '127.0.0.1', 6000,
                             proto_factory=TCompactProtocolFactory(), trans_factory=TBufferedTransportFactory())

        assert client.Test(req) == None


class TestBinaryFramed(object):
    def setup_class(self):
        server = make_server(oneway_thrift.echo, Dispatcher(), '127.0.0.1', 6000,
                             proto_factory=TBinaryProtocolFactory(), trans_factory=TFramedTransportFactory())
        self.p = multiprocessing.Process(target=server.serve)
        self.p.start()
        time.sleep(1)  # Wait a second for server to start.

    def teardown_class(self):
        self.p.terminate()

    def test_echo(self):
        req = "Hello!"
        client = make_client(oneway_thrift.echo, '127.0.0.1', 6000,
                             proto_factory=TBinaryProtocolFactory(), trans_factory=TFramedTransportFactory())

        assert client.Test(req) == None


class TestCompactFramed(object):
    def setup_class(self):
        server = make_server(oneway_thrift.echo, Dispatcher(), '127.0.0.1', 6000,
                             proto_factory=TCompactProtocolFactory(), trans_factory=TFramedTransportFactory())
        self.p = multiprocessing.Process(target=server.serve)
        self.p.start()
        time.sleep(1)  # Wait a second for server to start.

    def teardown_class(self):
        self.p.terminate()

    def test_echo(self):
        req = "Hello!"
        client = make_client(oneway_thrift.echo, '127.0.0.1', 6000,
                             proto_factory=TCompactProtocolFactory(), trans_factory=TFramedTransportFactory())

        assert client.Test(req) == None
