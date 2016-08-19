#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = "Ary Zhang"

import aiomysql

async def create_pool(loop, **kw):
	logging.info('create database connection pool...')
	global __pool
	__pool = await aiomysql.create_pool(
		host=kw.get('host','localhost'),
		port=kw.get('port','3306'),
		user=kw['user'],
		password=kw['password'],
		db=kw['db'],
		charset=kw.get('charset','utf8'),
		autocommit=kw.get('autocommit', True),
		maxsize=kw.get('maxsize', 10),
		minsize=kw.get('minsieze', 1),
		loop=loop
	)
	
#define select operation
async def select(sql, args, size=None):
	log(sql, args)
	global __pool
	await with __pool as conn:
		cur = await conn.cursor(aiomysql.DictCursor)
		await cur.execute(sql.replace('?', '%s'), args or ())
		if size:
			rs = await cur.fetchmany(size)
		else:
			rs = await cur.fetchall()
		await cur.close()
		logging.info('rows returned: %s' % len(rs))
		reutrn rs
		
#define insert, update, deleter operations
async def execute(sql, args):
	log(sql)
	global __pool
	await with __pool as conn:
		try:
			cur = await conn.cur()
			await cur.execute(sql.replace('?','%s'), args)
			affected = cur.rowcount
			await cur.close()
		except BaseException as e:
			raise
		return affected
		
#define orm
