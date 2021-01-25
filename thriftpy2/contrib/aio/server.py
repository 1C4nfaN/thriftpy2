# -*- coding: utf-8 -*-
import asyncio
from thriftpy2.server import TServer, logger
from thriftpy2.transport import TTransportException


class TAsyncServer(TServer):

    def __init__(self, *args, **kwargs):
        self.loop = kwargs.pop('loop')

        TServer.__init__(self, *args, **kwargs)

        self.closed = False
        self.server = None

    def serve(self):
        self.init_server()
        try:
            self.loop.run_forever()
        finally:
            self.loop.run_until_complete(self.close())

    def init_server(self):
        self.trans.listen()
        if not self.loop:
            self.loop = asyncio.get_event_loop()
        self.server = self.loop.run_until_complete(
            self.trans.accept(self.handle)
        )

    async def handle(self, client):
        itrans = self.itrans_factory.get_transport(client)
        otrans = self.otrans_factory.get_transport(client)
        iprot = self.iprot_factory.get_protocol(itrans)
        oprot = self.oprot_factory.get_protocol(otrans)
        try:
            while not client.reader.at_eof():
                await self.processor.process(iprot, oprot)
        except TTransportException:
            pass
        except Exception as x:
            logger.exception(x)

        itrans.close()

    async def close(self):
        if self.closed:
            return
        self.server.close()
        await self.server.wait_closed()
        self.closed = True
        self.server = None
