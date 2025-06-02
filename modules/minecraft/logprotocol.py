# -*- coding: utf-8 -*-

# RelayBot - Simple Relay Service, modules/minecraft/logprotocol.py
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
import asyncio
import re
import os

log = _logging.log.getChild(__name__)

clients = {}
players = {}

class MCLogProtocol:
	def __init__(self, loop, config, module):
		global clients, players
		self.loop = loop
		self.config = config
		self.module = module
		self.log = log.getChildObj(self.config['name'])
		self.isshutdown = False

		players[self.config['name']] = {}

		self.logre = re.compile(r'^\[(?P<time>[^\]]+)\] \[(?P<thread>[^\]]+?)(?: #[0-9]+)?/(?P<level>[A-Z]+)\]: (?P<message>[^\r\n]+)$')
		self.msgcb = {
			'PLAYER_CONNECT': self.e_player_connect,
			'PLAYER_DISCONNECT': self.e_player_disconnect,
			'MESSAGE': self.e_message,
			'ACTION': self.e_action,
			'ADVANCEMENT': self.e_advancement,
			'DEATH': self.e_death,
		}

		clients[self.config['name']] = self

	async def parse_log(self):
		log_file_path = self.config['log']['file']
		if not os.path.exists(log_file_path):
			self.log.error(f"Log file {log_file_path} does not exist.")
			return

		self.log.info(f"Starting to parse log file: {log_file_path}")
		with open(log_file_path, 'r', encoding='utf-8') as log_file:
			log_file.seek(0, os.SEEK_END)  # Start at the end of the file
			while not self.isshutdown:
				current_position = log_file.tell()
				line = log_file.readline()
				if not line:
					await asyncio.sleep(1)  # Wait for new lines
					log_file.seek(0, os.SEEK_END)  # Check if file has grown
					if log_file.tell() < current_position:  # Log rotation detected
						self.log.info("Log rotation detected, restarting from beginning of file.")
						log_file.seek(0)
					continue
				self.log.debug(f"Read line: {line.strip()}")
				match = self.logre.match(line)
				if match:
					self._handle_msg(match.groupdict())
				else:
					self.log.warning(f"Unable to parse log line: {line.strip()}")

	def _handle_msg(self, msg):
		for event, callback in self.msgcb.items():
			if event in msg['message']:
				callback(msg)
				break

	def shutdown(self, loop):
		self.log.info("Shutting down log parser")
		self.isshutdown = True
		if self.config['name'] in clients:
			del clients[self.config['name']]

	def e_player_connect(self, msg):
		self.log.info(f"Player connected: {msg}")

	def e_player_disconnect(self, msg):
		self.log.info(f"Player disconnected: {msg}")

	def e_message(self, msg):
		self.log.info(f"Message: {msg}")

	def e_action(self, msg):
		self.log.info(f"Action: {msg}")

	def e_advancement(self, msg):
		self.log.info(f"Advancement: {msg}")

	def e_death(self, msg):
		self.log.info(f"Death: {msg}")

async def connectclient(loop, conf, module):
	try:
		log.info(f"Starting log parser for {conf['name']}")
		protocol = MCLogProtocol(loop, conf, module)
		await protocol.parse_log()
	except Exception as e:
		log.warning(f"Exception occurred while parsing log for {conf['name']}: {str(e)}")
		log.info("Retrying in 30 seconds")
		loop.call_later(30, createclient, loop, conf, module)

def createclient(loop, conf, module):
	loop.create_task(connectclient(loop, conf, module))
