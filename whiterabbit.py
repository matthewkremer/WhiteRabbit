from __future__ import division
import copy, pymongo
from bson.objectid import ObjectId
from bson.json_util import dumps

"""
	Generic Use:

		from fimedlabs.reveal.whiterabbit2 import WhiteRabbit, Sum, Avg, Min, Max, Count, SubJoin

	BEGIN SETTINGS
"""

connection = pymongo.MongoClient('',0)
db_name = ''

# Use this function to modify all queries that are sent to the database before they are sent.
def modify_query(obj):
	if 'typeValues' in obj:
		if type(obj['typeValues']) == list:
			size = len(obj['typeValues'])
			for i in xrange(size):
				for j in xrange(len(obj['typeValues'][i])):
					obj['typeValues.'+str(i)+'.'+str(j)] = obj['typeValues'][i][j]
			obj['typeValues'] = {
				'$size': size
			}
	return obj

# Use this function to take in a key value and return True/False depending on whether or not that key should be an ObjectId.
# This allows you to pass in string values that will then be converted to an ObjectId for querying.
def isID(key):
	return (('_id' == key or key[-2:] == 'ID') and key != "clientID" and key != "clientProviderID")

"""
	END SETTINGS
"""

class WhiteRabbit:

	"""

	"""

	def __init__(self, *args, **kwargs):
		require = kwargs.pop('require', {})
		trending = kwargs.pop('trending', {})

		self._client = connection
		self._db_name = db_name
		self._db = self._client[self._db_name]
		self._inserts = {}
		self._require = require
		self._trending = trending

	def require(self, n=None):
		if n:
			self._require = n
			return self
		else:
			return self._require

	def db(self, n=None):
		if n:
			self._db_name = n
			self._db = self._client[self._db_name]
			return self
		else:
			return self._db

	def _innerFind(self, collection="", query={}, require=True, modify=True, projection=None, distinctList=None, trending=None):
		query = _modify_obj(self._require, query, modify_obj=modify, useRequires=require, trending=trending)
		collection = self._db[collection]
		results = collection.find(query)

		if projection:
			results = list(results)
			for i in xrange(0,len(results)):
				results[i] = _store({},projection,results[i])

		if distinctList:
			results = results.distinct(distinctList)
			return results

		return self.toWhiteRabbitList(results, trending=trending)

	def find(self, collection="", query={}, require=True, modify=True, projection=None, distinctList=None, trending=None):
		if self._trending or trending:
			if trending:
				useTrending = trending
			else:
				useTrending = self._trending
			results = {}
			for key, value in useTrending.iteritems():
				results[key] = self._innerFind(collection=collection, query=query, require=require, modify=modify, projection=projection, distinctList=distinctList, trending=value)
			return TrendingWhiteRabbitList(results)
		else:
			return self._innerFind(collection=collection, query=query, require=require, modify=modify, projection=projection, distinctList=distinctList)

	def findOne(self, collection="", query={}, require=True, modify=True, projection=None, trending=None):
		query = _modify_obj(self._require, query, modify_obj=modify, useRequires=require)
		collection = self._db[collection]
		result = collection.find_one(query)
		if projection:
			result = _store({},projection,result)
		return result

	def toTrendingWhiteRabbitList(self, results):
		return TrendingWhiteRabbitList(results)

	def toWhiteRabbitList(self, l, trending=None):
		return WhiteRabbitList(l,self._db,self._require, trending)

	def listQueryStart(self,on):
		return WhiteRabbitListQuery(self._db_name, on, self._require)

	def delayInsert(self, collection, item):
		item = _modify_obj(self._require, item,False)
		if collection not in self._inserts:
			self._inserts[collection] = []
		self._inserts[collection].append(item)

	def completeInsert(self):
		for key, value in self._inserts.iteritems():
			self._db[key].insert(value)
			self._inserts[key] = []

class WhiteRabbitListQuery:

	"""

	"""

	def __init__(self, db, on, require={}):
		self._client = connection
		self._db_name = db
		self._db = self._client[db]
		self.list = []
		self._list_init = False
		self._list_store = {}
		self._list_on = on
		self._require = require

	def copy(self):
		new = WhiteRabbitListQuery(self._db_name,self._list_on,self._require)
		new.list = copy.deepcopy(self.list)
		new._list_store = copy.deepcopy(self._list_store)
		new._list_init = copy.deepcopy(self._list_init)
		return new

	def query(self, collection="", query={}, store_to=False):
		if store_to:
			self = self.copy()
			query = _modify_obj(self._require, query)
			collection = self._db[collection]
			res = collection.find(query).distinct(self._list_on)
			self._list_store[store_to] = res
		else:
			self = self.qAnd(collection,query)
		return self

	def listStore(self, l, store_to):
		self._list_store[store_to] = l
		return self

	def qAndList(self, l):
		self = self.copy()
		res = l
		if len(self.list) == 0 and not self._list_init:
			self.list = res
			self._list_init = True
		else:
			self.list = _and(self.list,res)
		return self

	def qOrList(self, l):
		self = self.copy()
		res = l
		if len(self.list) == 0 and not self._list_init:
			self.list = res
			self._list_init = True
		else:
			self.list = _or(self.list,res)
		return self

	def qAnd(self, collection, query={}):
		self = self.copy()
		query = _modify_obj(self._require, query)
		collection = self._db[collection]
		res = collection.find(query).distinct(self._list_on)
		if len(self.list) == 0 and not self._list_init:
			self.list = res
			self._list_init = True
		else:
			self.list = _and(self.list,res)
		return self

	def qOr(self, collection, query={}):
		self = self.copy()
		query = _modify_obj(self._require, query)
		collection = self._db[collection]
		res = collection.find(query).distinct(self._list_on)
		if len(self.list) == 0 and not self._list_init:
			self.list = res
			self._list_init = True
		else:
			self.list = _or(self.list,res)
		return self

	def logic(self, logic):
		self = self.copy()
		self.list = logic_parser(logic,self._list_store)
		return self

	def getObjects(self,collection="", query=None, field="_id", require=True, modify=True, projection=None):
		collection = self._db[collection]
		if not query:
			results = list(collection.find({field: {'$in': self.list}}))
		else:
			query = _modify_obj(self._require, query, modify_obj=modify, useRequires=require)
			if field:
				query[field] = {'$in': self.list}
			results = list(collection.find(query))
			
		if projection:
			for i in xrange(0,len(results)):
				results[i] = _store({},projection,results[i])

		return WhiteRabbitList(results,self._db,self._require)

class TrendingWhiteRabbitList:

	"""

	"""

	def __init__(self, results):
		self.results = results

	def aggregate(self, *args, **kwargs):
		leftKey = kwargs.pop('leftKey',"_id")
		rightKey = kwargs.pop('rightKey',"_id")
		emitRightKey = kwargs.pop('emitRightKey',None)
		collection = kwargs.pop('collection',"")
		query = kwargs.pop('query',{})
		require = kwargs.pop('require', True)
		modify = kwargs.pop('modify', True)
		subJoin = kwargs.pop('subJoin', None)
		trending = kwargs.pop('trending', None)

		for key, value in self.results.iteritems():
			print key
			passTrending = None
			if key in trending:
				passTrending = trending[key]
			myquery = copy.deepcopy(query)
			self.results[key].aggregate(*args, leftKey=leftKey, rightKey=rightKey, emitRightKey=emitRightKey, collection=collection, query=myquery, require=require, modify=modify, subJoin=subJoin, trending=passTrending)

		return self

	def join(self, *args, **kwargs):
		leftKey = kwargs.pop('leftKey', '_id')
		rightKey = kwargs.pop('rightKey', '_id')
		emitRightKey = kwargs.pop('emitRightKey', None)
		collection = kwargs.pop('collection', '')
		query = kwargs.pop('query', {})
		require = kwargs.pop('require', True)
		modify = kwargs.pop('modify', True)
		projection = kwargs.pop('projection', None)
		storeTo = kwargs.pop('storeTo', None)
		expectList = kwargs.pop('expectList', False)
		listSortOn = kwargs.pop('listSortOn', '')
		listSortDir = kwargs.pop('listSortDir', 'asc')
		default = kwargs.pop('default', None)
		subJoin = kwargs.pop('subJoin', None)
		trending = kwargs.pop('trending', None)

		for key, value in self.results.iteritems():
			passTrending = None
			if key in trending:
				passTrending = trending[key]
			myquery = copy.deepcopy(query)
			self.results[key].aggregate(leftKey=leftKey, rightKey=rightKey, emitRightKey=emitRightKey, collection=collection, query=myquery, require=require, modify=modify, projection=projection, storeTo=storeTo, expectList=expectList, listSortOn=listSortOn, listSortDir=listSortDir, defaul=default, subJoin=subJoin, trending=passTrending)

		return self

	def sort(self, sortBy="", sortDir="asc", sortFunc=None):
		for key, value in self.results.iteritems():
			self.results[key].sort(sortBy, sortDir, sortFunc)

	"""
		I'm not actually sure how paginating trending is going to work, so commenting out for now.
		Go through most recent? Then replace the previous sets to only include those??
	"""
	# def paginate(self, page=0,limit=50):
	# 	for key, value in self.results.iteritems():
	# 		self.results[key].paginate(page, limit)

class WhiteRabbitList(list):

	def __init__(self,*args,**kwargs):
		super(WhiteRabbitList, self).__init__(args[0])
		self.connection = args[1]
		self.require = args[2]
		try:
			self.trending = args[3]
		except:
			self.trending = False

	def aggregate(self, *args, **kwargs):
		leftKey = kwargs.pop('leftKey',"_id")
		rightKey = kwargs.pop('rightKey',"_id")
		emitRightKey = kwargs.pop('emitRightKey',None)
		collection = kwargs.pop('collection',"")
		query = kwargs.pop('query',{})
		require = kwargs.pop('require', True)
		modify = kwargs.pop('modify', True)
		subJoin = kwargs.pop('subJoin', None)
		trending = kwargs.pop('trending', None)

		ids = [_get(leftKey,item) for item in self]
		collection = self.connection[collection]
		query = _modify_obj(self.require, query, modify_obj=modify, useRequires=require, trending=trending)

		if not subJoin:
			query[rightKey] = {
				'$in': ids
			}

		if collection:
			results = collection.find(query)
		else:
			raise WhiteRabbitJoinException("You must specify the collection for the right join.")

		if subJoin:
			results = WhiteRabbitList(list(results),self.connection,self.require)
			if subJoin.__class__ is not list:
				subJoin = [subJoin]
			for j in subJoin:
				results.join(**j.options)

		if emitRightKey:
			newResults = []
			for item in results:
				for emit in emitRightKey(item):
					if emit in ids:
						newResults.append(item)
						break
		else:
			newResults = [item for item in results if _get(rightKey,item) in ids]
		results = newResults

		resultsDict = {}
		for item in results:
			if emitRightKey:
				for emit in emitRightKey(item):
					if emit in resultsDict:
						resultsDict[emit].append(item)
					else:
						resultsDict[emit] = [item]
			else:
				check = _get(rightKey, item)
				if check in resultsDict:
					resultsDict[check].append(item)
				else:
					resultsDict[check] = [item]

		print args

		for agg in args:
			options = agg.options
			key = options['key']
			storeTo = options['storeTo']
			default = options['default']
			distinct = options['distinct']
			type = options['type']

			print "HERE"

			if not key and type != "count":
				raise WhiteRabbitJoinException("You must specify the key on which to perform this aggregation.")
			if not storeTo:
				raise WhiteRabbitJoinException("You must specify storeTo to your specific aggregation.")
			if not type:
				raise WhiteRabbitJoinException("Do not access aggregate() class directly, instead use subclasses: Count, Sum, Avg, Max and Min.")

			for item in self:
				checkDistinct = []
				keyVal = _get(leftKey, item)
				if keyVal in resultsDict:
					if type == "count":
						if distinct:
							storeVal = 0
							for i in resultsDict[keyVal]:
								check = _get(distinct,i)
								if check not in checkDistinct:
									storeVal += 1
									checkDistinct.append(check)
						else:
							storeVal = len(resultsDict[keyVal])
					elif type == "sum":
						storeVal = 0
						for sumMe in resultsDict[keyVal]:
							storeVal += _get(key, sumMe)
					elif type == "avg":
						storeVal = 0
						count = 0
						for avgMe in resultsDict[keyVal]:
							count += 1
							storeVal += _get(key, avgMe)
						storeVal = storeVal / count
					elif type == "max":
						storeVal = None
						for maxMe in resultsDict[keyVal]:
							check = _get(key, maxMe)
							if storeVal == None:
								storeVal = check
							else:
								if check > storeVal:
									storeVal = check
					elif type == "min":
						storeVal = None
						for minMe in resultsDict[keyVal]:
							check = _get(key, minMe)
							if storeVal == None:
								storeVal = check
							else:
								if check < storeVal:
									storeVal = check
				else:
					storeVal = default
				item = _store(item,{storeTo: 'storeVal'},{'storeVal': storeVal})

		return self

	def join(self, *args, **kwargs):
		leftKey = kwargs.pop('leftKey', '_id')
		rightKey = kwargs.pop('rightKey', '_id')
		emitRightKey = kwargs.pop('emitRightKey', None)
		collection = kwargs.pop('collection', '')
		query = kwargs.pop('query', {})
		require = kwargs.pop('require', True)
		modify = kwargs.pop('modify', True)
		projection = kwargs.pop('projection', None)
		storeTo = kwargs.pop('storeTo', None)
		expectList = kwargs.pop('expectList', False)
		listSortOn = kwargs.pop('listSortOn', '')
		listSortDir = kwargs.pop('listSortDir', 'asc')
		default = kwargs.pop('default', None)
		subJoin = kwargs.pop('subJoin', None)

		if listSortOn:
			if listSortDir == "asc":
				listSortDir = False
			else:
				listSortDir = True

		ids = [_get(leftKey,item) for item in self]
		collection = self.connection[collection]

		if not subJoin:
			query[rightKey] = {
				'$in': ids
			}

		query = _modify_obj(self.require, query, modify_obj=modify, useRequires=require)

		if collection:
			import time
			start = time.time()
			results = collection.find(query)				
			totalTime = time.time() - start
		else:
			raise WhiteRabbitJoinException("You must specify the collection for the right join.")

		if subJoin:
			results = WhiteRabbitList(list(results),self.connection,self.require)
			if subJoin.__class__ is not list:
				subJoin = [subJoin]
			for j in subJoin:
				results.join(**j.options)

		if emitRightKey:
			newResults = []
			for item in results:
				for emit in emitRightKey(item):
					if emit in ids:
						newResults.append(item)
						break
		else:
			newResults = [item for item in results if _get(rightKey,item) in ids]
		results = newResults

		resultsDict = {}
		for item in results:
			check = _get(rightKey, item)
			if check in resultsDict:
				if expectList:
					resultsDict[check].append(item)
				else:

					raise WhiteRabbitJoinException('Your join returned multiple right-side objects for one left-side object without expectList being set to True. %s' % check)
			else:
				if expectList:
					resultsDict[check] = [item]
				else:
					resultsDict[check] = item

		for item in self:
			check = _get(leftKey,item)
			if check in resultsDict:
				result = resultsDict[check]
			else:
				result = default

			if storeTo and not projection:
				item[storeTo] = result
				if expectList and listSortOn:
					item[storeTo] = sorted(item[storeTo], key=lambda obj: _get(listSortOn,obj), reverse=listSortDir)
			elif projection:
				if expectList:
					if storeTo:
						addList = []
						for make in result:
							addList.append(_store({},projection,make))
						item[storeTo] = addList

						if listSortOn:
							item[storeTo] = sorted(item[storeTo], key=lambda obj: _get(listSortOn,obj), reverse=listSortDir)
					else:
						raise WhiteRabbitJoinException("When expectList is set to true, you must store the returned list by setting storeTo.")
				else:
					if storeTo:
						item[storeTo] = _store({},projection,result)
					else:
						item = _store(item,projection,result)
			else:
				raise WhiteRabbitJoinException("You must set either projection or storeTo in order to add the joined data to the left objects.")

		return self

	def sort(self, sortBy="", sortDir="asc", sortFunc=None):
		if sortBy:
			if sortDir == "asc":
				sortDir = False
			else:
				sortDir = True
			if not sortFunc:
				new = WhiteRabbitList(sorted(self,key=lambda obj: _get(sortBy,obj),reverse=sortDir),self.connection,self.require)
			else:
				new = WhiteRabbitList(sorted(self,key=lambda obj: sortFunc(_get(sortBy,obj)),reverse=sortDir),self.connection,self.require)
			del self[:]
			self.extend(new)
		return self

	def paginate(self, page=0,limit=50):
		new = WhiteRabbitList(self[page*limit:page*limit+limit],self.connection,self.require)
		del self[:]
		self.extend(new)
		return self


class Aggregate(object):

	def __init__(self, key="", storeTo="", default=None, distinct="", type=""):
		self.options = {
			'key': key,
			'storeTo': storeTo,
			'default': default,
			'distinct': distinct,
			'type': type
		}

class Sum(Aggregate):

	def __init__(self, key="", storeTo="", default=0, distinct=""):
		super(Sum, self).__init__(key=key, storeTo=storeTo, default=default, distinct=distinct, type="sum")

class Avg(Aggregate):

	def __init__(self, key="", storeTo="", default=0, distinct=""):
		super(Avg, self).__init__(key=key, storeTo=storeTo, default=default, distinct=distinct, type="avg")

class Max(Aggregate):

	def __init__(self, key="", storeTo="", default=None, distinct=""):
		super(Max, self).__init__(key=key, storeTo=storeTo, default=default, distinct=distinct, type="max")

class Min(Aggregate):

	def __init__(self, key="", storeTo="", default=None, distinct=""):
		super(Min, self).__init__(key=key, storeTo=storeTo, default=default, distinct=distinct, type="min")

class Count(Aggregate):

	def __init__(self, key="", storeTo="", default=0, distinct=""):
		super(Count, self).__init__(key=key, storeTo=storeTo, default=default, distinct=distinct, type="count")

class SubJoin():

	def __init__(self, **kwargs):
		options = {
			'leftKey': "_id",
			'rightKey': "_id",
			'collection': "",
			'query': {},
			'require': True,
			'modify': True,
			'projection': None,
			'storeTo': None,
			'expectList': False,
			'listSortOn': "",
			'listSortDir': "asc",
			'default': None,
			'subJoin': None
		}
		options.update(kwargs)
		self.options = options

class WhiteRabbitJoinException(Exception):
	pass

def _store(obj, instruction, result):
	if type(instruction) == dict:
		for key, value in instruction.iteritems():
			if type(value) == str:
				obj[key] = _get(value, result)
			elif type(value) == list:
				if key not in obj:
					obj[key] = []
				obj[key] = _store(obj[key], value, result)
			elif type(value) == dict:
				if key not in obj:
					obj[key] = {}
				obj[key] = _store(obj[key], value, result)
	elif type(instruction) == list:
		for item in instruction:
			if type(item) == str:
				obj.append(_get(item, result))
			elif type(item) == list:
				obj.append(_store([], item, result))
			elif type(item) == dict:
				obj.append(_store({}, item, result))
	return obj

def _get(s,result):
	if s == '$object':
		return result
	s = s.split('.')
	on = result
	for follow in s:
		if type(on) == list:
			on = on[int(follow)]
		else:
			on = on[follow]
	return on

def logic_parser(s,d):
	str_to_token = copy.deepcopy(d)
	str_to_token['and'] = lambda left, right: _and(left,right)
	str_to_token['or'] = lambda left, right: _or(left,right)
	str_to_token['('] = '('
	str_to_token[')'] = ')'

	empty_res = []

	def create_token_lst(s):
		s = s.replace('(',' ( ')
		s = s.replace(')',' ) ')
		return [str_to_token[it] for it in s.split()]

	def find(lst, what, start=0):
	    return [i for i,it in enumerate(lst) if it == what and i >= start]


	def parens(token_lst):
	    """returns:
	        (bool)parens_exist, left_paren_pos, right_paren_pos
	    """
	    left_lst = find(token_lst, '(')

	    if not left_lst:
	        return False, -1, -1

	    left = left_lst[-1]

	    #can not occur earlier, hence there are args and op.
	    right = find(token_lst, ')', left + 4)[0]

	    return True, left, right


	def bool_eval(token_lst):
	    """token_lst has length 3 and format: [left_arg, operator, right_arg]
	    operator(left_arg, right_arg) is returned"""

	    if len(token_lst) == 1:
	    	return token_lst[0]

	    token_lst[0:3] = [token_lst[1](token_lst[0], token_lst[2])]

	    return bool_eval(token_lst)


	def formatted_bool_eval(token_lst, empty_res=empty_res):
	    """eval a formatted (i.e. of the form 'ToFa(ToF)') string"""
	    if not token_lst:
	        return empty_res

	    if len(token_lst) == 1:
	        return token_lst[0]

	    has_parens, l_paren, r_paren = parens(token_lst)

	    if not has_parens:
	        return bool_eval(token_lst)

	    token_lst[l_paren:r_paren + 1] = [bool_eval(token_lst[l_paren+1:r_paren])]

	    return formatted_bool_eval(token_lst, bool_eval)

	return formatted_bool_eval(create_token_lst(s))

def _and(l1,l2):
	return [i for i in l1 if i in l2]

def _or(l1,l2):
	return list(set(l1+l2))

def _modify_obj(require, obj, modify_obj=True, useRequires=True, trending=False):
	print trending
	if useRequires:
		for key, value in require.iteritems():
			if key not in obj:
				obj[key] = value
	if trending:
		for key, value in trending.iteritems():
			if key not in obj:
				obj[key] = value
				print key
				print value
	if modify_obj:
		obj = modify_query(obj)
	for key, value in obj.iteritems():
		if type(value) is str and isID(key):
			obj[key] = ObjectId(value)
		if type(value) is list and isID(key):
			for v in value:
				v = ObjectId(value)
	return obj
