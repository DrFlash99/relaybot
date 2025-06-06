# -*- coding: utf-8 -*-

# RelayBot - Simple Relay Service, modules/minecraft/rconprotocol.py
#
# Copyright (C) 2023 Matthew Beeching
#
# This file is part of RelayBot.
#
# RelayBot is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# RelayBot is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with RelayBot.  If not, see <http://www.gnu.org/licenses/>.

import core.logging as _logging
import core.modules as _modules
import asyncio, binascii, queue, re, struct

log = _logging.log.getChild(__name__)

clients = {}

class MCRConProtocol(asyncio.Protocol):
	def __init__(self, loop, config, module):
		global clients
		self.loop = loop
		self.config = config
		self.module = module
		self.transport = None
		self.log = log.getChildObj(self.config['name'])
		self.id = 0

		self.buf = b''

		self.rconcallbacks = {}
		self.rconcallbacks[-1] = self._rcon_login_failure;
		self.rconqueue = queue.Queue()
		self.rconwaitid = -1

		self.listre = {
			'main': re.compile('^There are \d+ of a max of \d+ players online: (?P<list>.*?)$'),
			'player': re.compile('^(?P<name>.+?) \\((?P<uuid>[-a-f0-9]+?)\\)$')
			}

		self.isshutdown = False

		clients[self.config['name']] = self

	def connection_made(self, transport):
		self.transport = transport
		self.log.debug('Connected to RCON, sending login')
		self._sendcmd(self.config['rcon']['password'], type=3, callback=self._rcon_login_callback)

	def connection_lost(self, exc):
		global clients
		if not exc is None:
			self.log.info('Lost connection: ' + str(exc))
		else:
			self.log.info('Lost connection')
		if self.config['name'] in clients:
			del clients[self.config['name']]

		if self.isshutdown:
			return

		self.log.info('Reconnecting in 30 seconds')
		self.loop.call_later(30, createclient, self.loop, self.config, self.module)

	def eof_received(self):
		self.log.debug('EOF received')

	def data_received(self, data):
		self.log.protocol('Received RCON data: ' + binascii.hexlify(data).decode('utf-8'))
		self.buf = self.buf + data

		while True:
			if len(self.buf) < 4:
				break

			(length,) = struct.unpack('<i', self.buf[:4])

			if len(self.buf) < (length + 4):
				break

			dbuf = self.buf[:length + 4]
			self.buf = self.buf[length + 4:]

			self.log.protocol('Parsing RCON packet: ' + binascii.hexlify(dbuf).decode('utf-8'))

			pkt = self._rcondecode(dbuf)
			self.log.protocol('Parsed RCON packet: ' + str(pkt))

			if pkt['id'] in self.rconcallbacks:
				self.rconcallbacks[pkt['id']](pkt)
				del self.rconcallbacks[pkt['id']]
			# TODO: handle payload fragmentation (4096 max size payload)

			if self.rconwaitid != pkt['id']:
				self.log.warning("Received unexpected RCON reply")
			self.rconwaitid = -1
			self._sendnextcmd()

	def shutdown(self, loop):
		self.isshutdown = True
		self.transport.close()

	def handle_event(self, loop, module, sender, protocol, event, data):
		if event != 'RCON_SENDCMD':
			return

		if not 'command' in data:
			self.log.warning('Event ' + event + ' missing command to execute')
			return

		cb = None
		cmd = data['command']

		if 'callback' in data and data['callback'] is not None:
			cb = data['callback']

		self._sendcmd(cmd, callback=cb)

	def _rcon_login_callback(self, pkt):
		if pkt['type'] != 2:
			return
		del self.rconcallbacks[-1]
		self.log.info('RCON login successful')
		self._sendcmd('list uuids', callback=self._rcon_list_uuids)

	def _rcon_login_failure(self, pkt):
		if pkt['type'] != 2:
			return
		self.log.warning('RCON login failed, password incorrect?')
		self.log.info('Reconnecting in 30 seconds')
		self.loop.call_later(30, createclient, self.loop, self.config, self.module)

	def _rcon_list_uuids(self, pkt):
		payload = pkt['payload'].decode('utf-8')
		match = self.listre['main'].match(payload)
		if match:
			players = match.group('list')
			_modules.send_event(self.loop, self.module, self.config['name'], 'rcon', 'PLAYERS_OFFLINE', None)
			for player in players.split(', '):
				matchp = self.listre['player'].match(player)
				if matchp:
					puuid = {'name': matchp.group('name'), 'uuid': matchp.group('uuid')}
					pip = {'name': matchp.group('name'), 'ip': '0.0.0.0', 'port': '0'}
					pcon = {'name': matchp.group('name'), 'uuid': matchp.group('uuid'), 'ip': '0.0.0.0', 'port': '0', 'message': 'joined the game'}

					self.loop.call_soon(_modules.send_event, self.loop, self.module, self.config['name'], 'rcon', 'PLAYER_UUID', puuid)
					self.loop.call_soon(_modules.send_event, self.loop, self.module, self.config['name'], 'rcon', 'PLAYER_IP', pip)
					self.loop.call_soon(_modules.send_event, self.loop, self.module, self.config['name'], 'rcon', 'PLAYER_CONNECT', pcon)
		return

	def _resetid(self):
		self.id = 0

	def _sendcmd(self, cmd, type=2, callback=None):
		qcmd = self.id, self._rconpacket(self.id, type, cmd), callback
		self.id += 1

		self.log.protocol('Queued RCON packet: ' + binascii.hexlify(qcmd[1]).decode('utf-8'))
		self.rconqueue.put(qcmd)

		self._sendnextcmd()

		return qcmd[0]

	def _sendnextcmd(self):
		if self.rconwaitid >= 0:
			return
		if self.rconqueue.empty():
			return

		qcmd = self.rconqueue.get()
		self.rconwaitid = qcmd[0]

		if qcmd[2] is not None:
			self.rconcallbacks[qcmd[0]] = qcmd[2]
		self.log.protocol('Sending RCON packet: ' + binascii.hexlify(qcmd[1]).decode('utf-8'))
		self.transport.write(qcmd[1])
		self.log.protocol('Parsed RCON packet: ' + str(self._rcondecode(qcmd[1])))

		return

	def _rconpacket(self, id=0, type=0, payload=None):
		pkt = struct.pack('<ii', id, type)
		if payload != None:
			pkt += payload.encode('utf-8')
		else:
			payload = ''
		pkt += b'\x00\x00'

		pkt = struct.pack('<i', len(pkt)) + pkt

		return pkt

	def _rcondecode(self, raw):
		pkt = {'id': -1, 'type': -1, 'payload': 'ERROR'}

		try:
			(length,) = struct.unpack('<i', raw[:4])
			fmt = '<ii%ds2s' % (length-10)
			(pkt['id'], pkt['type'], pkt['payload'], pad,) = struct.unpack(fmt, raw[4:])
		except Exception as e:
			self.log.warning('Exception unpacking RCON packet: ' + str(e))
		return pkt

async def connectclient(loop, conf, module):
	try:
		log.info('Connecting RCON client ' + conf['name'] + ' to ' + '[' + conf['rcon']['host'] + ']:' + conf['rcon']['port'])
		await loop.create_connection(lambda: MCRConProtocol(loop, conf, module), conf['rcon']['host'], conf['rcon']['port'])
	except Exception as e:
		log.warning('Exception occurred attempting to connect RCON client ' + conf['name'] + ': ' + str(e))
		log.info('Reconnecting in 30 seconds')
		loop.call_later(10, createclient, loop, conf, module)
	return

def createclient(loop, conf, module):
	loop.create_task(connectclient(loop, conf, module))
